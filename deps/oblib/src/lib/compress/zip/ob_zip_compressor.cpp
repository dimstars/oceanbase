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

#include <new>
#include "zip.h"
#include "lib/compress/zip/ob_zip_compressor.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace common {
const char* ObZipCompressor::compressor_name = "zip";
ObZipCompressor::ObZipCompressor()
		: zip_(nullptr),
		  file_name_()
{}
ObZipCompressor& ObZipCompressor::get_instance()
{
  static ObZipCompressor instance_;
  return instance_;
}

int ObZipCompressor::compress(const char* src_buffer, const size_t src_data_size, char* dst_buffer,
    size_t& dst_data_size, ZipCompressFlag flag)
{
  int ret = OB_SUCCESS;

	if (FAKE_FILE_HEADER > flag || TAIL < flag) {
		LIB_LOG(ERROR, "invalid zip compress flag", K(flag));
		ret = OB_ERROR;
	} else {
		if (FAKE_FILE_HEADER == flag) {
			zip_ = zip_stream_open(NULL, 0, ZIP_DEFAULT_COMPRESSION_LEVEL, 'w');
			// get a fake file header and the real file name
			if (file_name_.empty()) {
				LIB_LOG(ERROR, "zip file name is empty");
				ret = OB_ERROR;
			} else {
				zip_entry_open_out(zip_, file_name_.c_str(), dst_buffer, &dst_data_size);
			}
		} else if (DATA == flag) {
			zip_entry_write_out(zip_, src_buffer, src_data_size, dst_buffer, &dst_data_size);
		} else if (LAST_DATA == flag) {
			zip_entry_close_before(zip_, dst_buffer, &dst_data_size);
		} else if (FILE_HEADER == flag) {
			zip_entry_close_out(zip_, dst_buffer, &dst_data_size);
		} else {
			zip_stream_close_out(zip_, dst_buffer, &dst_data_size);
		}
	}

  return ret;
}

// Not Support
int ObZipCompressor::decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    const int64_t dst_buffer_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;

  UNUSED(src_buffer);
  UNUSED(dst_buffer);
  UNUSED(dst_buffer_size);
  dst_data_size = src_data_size;

  return ret;
}

const char* ObZipCompressor::get_compressor_name() const
{
  return compressor_name;
}

}  // namespace common
}  // namespace oceanbase
