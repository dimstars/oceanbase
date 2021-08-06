/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <string>
#include <deque>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <sys/prctl.h>

#include "malloc.h"
#include "lib/oblog/ob_log_compressor.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/compress/ob_compressor_pool.h"

using namespace oceanbase::lib;

namespace oceanbase {
namespace common {
ObLogCompressor::ObLogCompressor()
  : is_inited_(false),
    has_stoped_(true),
    compressor_(NULL),
    compress_tid_(0)
{}

ObLogCompressor::~ObLogCompressor()
{}

int ObLogCompressor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_STDERR("The ObLogCompressor has been inited.\n");
  } else if (0 != pthread_mutex_init(&log_compress_mutex_, NULL)) {
    ret = OB_ERR_SYS;
    LOG_STDERR("Failed to pthread_mutex_init.\n");
  } else if (0 != pthread_cond_init(&log_compress_cond_, NULL)) {
    ret = OB_ERR_SYS;
    pthread_mutex_destroy(&log_compress_mutex_);
    LOG_STDERR("Failed to pthread_cond_init.\n");
  } else {
    ObCompressor *ptr = NULL;
    if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(ZSTD_1_3_8_COMPRESSOR, ptr))) {
      LOG_STDERR("Fail to get_compressor, err_code=%d.\n", ret);
    } else {
      compressor_ = ptr;
      has_stoped_ = false;
      if (OB_UNLIKELY(0 != pthread_create(&compress_tid_, NULL, ObLogCompressor::log_compress_thread, this))) {
        ret = OB_ERR_SYS;
        LOG_STDERR("Fail to create log compression thread.\n");
      } else {
        is_inited_ = true;
        LOG_STDOUT("Success to create thread %ld.\n", compress_tid_);
      }
    }
    if (ret) {
      pthread_mutex_destroy(&log_compress_mutex_);
      pthread_cond_destroy(&log_compress_cond_);
    }
  }
  if (!is_inited_) {
    destroy();
  }

  return ret;
}

void ObLogCompressor::destroy()
{
  is_inited_ = false;
  if (0 != compress_tid_) {
    pthread_mutex_lock(&log_compress_mutex_);
    file_list_.clear();
    pthread_cond_signal(&log_compress_cond_);
    pthread_mutex_unlock(&log_compress_mutex_);
    has_stoped_ = true;
    pthread_join(compress_tid_, NULL);
    compress_tid_ = 0;
    pthread_mutex_destroy(&log_compress_mutex_);
    pthread_cond_destroy(&log_compress_cond_);
  }
}

std::string ObLogCompressor::get_compression_file_name(const std::string &file_name)
{
  std::string compression_file_name;
  int size = file_name.size();
  if (size > 4 && file_name.substr(size - 4) == ".zst") {
    compression_file_name = file_name;
  } else if (size > 0) {
    compression_file_name = file_name + ".zst";
  }
  return compression_file_name;
}

ObCompressor * ObLogCompressor::get_compressor()
{
  return compressor_;
}

int ObLogCompressor::append_log(const std::string &file_name)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_STDERR("The ObLogCompressor has not been inited.\n");
  } else {
    pthread_mutex_lock(&log_compress_mutex_);
    file_list_.push_back(file_name);
    pthread_cond_signal(&log_compress_cond_);
    pthread_mutex_unlock(&log_compress_mutex_);
  }
  return ret;
}

int ObLogCompressor::log_compress_block(char* dest, size_t dest_size, const char* src, size_t src_size, size_t &return_size)
{
  int ret = OB_SUCCESS;
  int64_t size = -1;
  if (OB_FAIL(((ObCompressor *)compressor_)->compress(src, src_size, dest, dest_size, size))) {
    LOG_STDERR("Failed to compress, err_code=%d.\n", ret);
  } else {
    return_size = size;
  }
  return ret;
}

void * ObLogCompressor::log_compress_thread(void *arg)
{
  if (NULL == arg) {
    LOG_STDERR("log_compress_thread init failed due to invalid arguments.\n");
  } else {
    ObLogCompressor * compressor = reinterpret_cast<ObLogCompressor *>(arg);
    prctl(PR_SET_NAME, "syslog_compress");
    compressor->log_compress();
  }
  return NULL;
}

void ObLogCompressor::log_compress()
{
  static int sleep_us = 100 * 1000; // 100ms
  int src_size   = DEFAULT_COMPRESSION_BLOCK_SIZE;
  int dest_size  = DEFAULT_COMPRESSION_BUFFER_SIZE;
  char *src_buf  = (char *)malloc(src_size);
  char *dest_buf = (char *)malloc(dest_size);
  if (!src_buf || !dest_buf) {
    LOG_STDERR("Failed to malloc.\n");
  } else while (!has_stoped_) {
    std::string file_name;
    pthread_mutex_lock(&log_compress_mutex_);
    if (file_list_.empty()) {
      pthread_cond_wait(&log_compress_cond_, &log_compress_mutex_);
    }
    if (!has_stoped_ && !file_list_.empty()) {
      file_name = file_list_.front();
      file_list_.pop_front();
    }
    pthread_mutex_unlock(&log_compress_mutex_);

    if (has_stoped_ || file_name.empty() || 0 != access(file_name.c_str(), F_OK)) {
    } else {
      std::string compression_file_name = get_compression_file_name(file_name);
      FILE *input_file = NULL;
      FILE *output_file = NULL;
      if (NULL == (input_file = fopen(file_name.c_str(), "r"))) {
        LOG_STDERR("Fialed to fopen, err_code=%d.\n", errno);
      } else if (NULL == (output_file = fopen(compression_file_name.c_str(), "w"))) {
        fclose(input_file);
        LOG_STDERR("Fialed to fopen, err_code=%d.\n", errno);
      } else {
        size_t read_size = 0;
        size_t write_size = 0;
        int ret = OB_SUCCESS;
        if (src_buf && dest_buf) {
          while(OB_SUCC(ret) && !feof(input_file)) {
            if ((read_size = fread(src_buf, 1, src_size, input_file)) > 0) {
              if (OB_FAIL(log_compress_block(dest_buf, dest_size, src_buf, read_size, write_size))) {
                LOG_STDERR("Failed to log_compress_block, err_code=%d.\n", ret);
              } else if (write_size != fwrite(dest_buf, 1, write_size, output_file)) {
                ret = OB_ERR_SYS;
                LOG_STDERR("Failed to fwrite, err_code=%d.\n", errno);
              }
            }
            usleep(sleep_us);
          }
        }
        fclose(input_file);
        fclose(output_file);
        if (0 != access(file_name.c_str(), F_OK) || OB_SUCCESS != ret) {
          unlink(compression_file_name.c_str());
        } else {
          unlink(file_name.c_str());
        }
      }
    }
  }
  if (src_buf) {
    free(src_buf);
  }
  if (dest_buf) {
    free(dest_buf);
  }
}

}
}
