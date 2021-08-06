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

#ifndef OB_LOG_COMPRESSOR_H_
#define OB_LOG_COMPRESSOR_H_

#include <string>
#include <deque>
#include <pthread.h>

namespace oceanbase {
namespace common {
/* Log files are divided into blocks and then compressed. The default block size is 2M.*/
static const int32_t DEFAULT_COMPRESSION_BLOCK_SIZE = 2 * 1024 * 1024;
/* To prevent extreme cases where the files become larger after compression,
 * the size of the decompression buffer needs to be larger than the original data.
 * Specific size can refer to the ZSTD code implementation. */
static const int32_t DEFAULT_COMPRESSION_BUFFER_SIZE =  DEFAULT_COMPRESSION_BLOCK_SIZE + DEFAULT_COMPRESSION_BLOCK_SIZE/128 + 512 + 19;

class ObCompressor;

class ObLogCompressor final
{
public:
  ObLogCompressor();
  virtual ~ObLogCompressor();
  static void * log_compress_thread(void *args);
  static std::string get_compression_file_name(const std::string &file_name);
  int init();
  void destroy();
  int append_log(const std::string &file_name);
  void log_compress();
  ObCompressor * get_compressor();

private:
  int log_compress_block(char* dest, size_t dest_size, const char* src, size_t src_size, size_t &return_size);

private:
  bool is_inited_;
  bool has_stoped_;
  ObCompressor * compressor_;
  std::deque<std::string> file_list_;
  pthread_mutex_t log_compress_mutex_;
  pthread_cond_t log_compress_cond_;
  pthread_t compress_tid_;
};

}
}

#endif /* OB_LOG_COMPRESSOR_H_ */
