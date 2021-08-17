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

#ifndef OCEANBASE_COMMON_COMPRESS_ZIP_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_ZIP_COMPRESSOR_H_
#include <string>
#include "lib/compress/ob_compressor.h"

struct zip_t;

//#define COMPRESSOR_NAME "zip"
namespace oceanbase {
namespace common {
enum ZipCompressFlag {
	FAKE_FILE_HEADER,
	DATA,
	LAST_DATA,
	FILE_HEADER,
	TAIL
};

class ObZipCompressor final {
public:
	static ObZipCompressor& get_instance();
  int compress(const char* src_buffer, const size_t src_data_size, char* dst_buffer,
      size_t& dst_data_size, ZipCompressFlag flag);
  int decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
      const int64_t dst_buffer_size, int64_t& dst_data_size);
  const char* get_compressor_name() const;
	void set_file_name(std::string file_name) 
	{
		file_name_ = file_name;
	}
private:
  ObZipCompressor();
  virtual ~ObZipCompressor()
  {}
	struct zip_t *zip_;
	std::string file_name_;
  static const char* compressor_name;
};

}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_COMMON_COMPRESS_ZIP_COMPRESSOR_H_
