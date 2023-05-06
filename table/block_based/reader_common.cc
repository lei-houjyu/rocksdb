//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/reader_common.h"

#include "monitoring/perf_context_imp.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/hash.h"
#include "util/string_util.h"
#include "util/xxhash.h"
#include "util/debug_buffer.h"
#include <iostream>
#include "logging/auto_roll_logger.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {
void ForceReleaseCachedEntry(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle, true /* force_erase */);
}

Status VerifyBlockChecksum(ChecksumType type, const char* data,
                           size_t block_size, const std::string& file_name,
                           uint64_t offset) {
  PERF_TIMER_GUARD(block_checksum_time);
  // After block_size bytes is compression type (1 byte), which is part of
  // the checksummed section.
  size_t len = block_size + 1;
  // And then the stored checksum value (4 bytes).
  uint32_t stored = DecodeFixed32(data + len);
  // debug_buffer_mu.lock();
  // debug_buffer_ss << "[debug] verify checksum at offset: " << offset << ", block size:" << block_size << std::endl;
  // debug_buffer = debug_buffer_ss.rdbuf()->str().data();
  // debug_buffer_mu.unlock();
  DEBUG_STRUCT_SET(checksum_from_offset, offset);
  DEBUG_STRUCT_SET(checksum_from_size, block_size);
  // ROCKS_LOG_INFO(global_dboption->rubble_info_log, "verify checksum at offset: %lu, block size: %lu", offset, block_size);
  // printf("[debug] verify checksum at offset: %lu, block size: %lu\n", offset, block_size);
  // std::cout << "[debug] verify checksum at offset: " << offset << ", block size:" << block_size << std::endl;

  Status s;
  uint32_t computed = 0;
  switch (type) {
    case kNoChecksum:
      break;
    case kCRC32c:
      stored = crc32c::Unmask(stored);
      computed = crc32c::Value(data, len);
      break;
    case kxxHash:
      computed = XXH32(data, len, 0);
      break;
    case kxxHash64:
      computed = Lower32of64(XXH64(data, len, 0));
      break;
    default:
      s = Status::Corruption(
          "unknown checksum type " + ToString(type) + " from footer of " +
          file_name + ", while checking block at offset " + ToString(offset) +
          " size " + ToString(block_size));
  }
  if (s.ok() && stored != computed) {
    s = Status::Corruption(
        "block checksum mismatch: stored = " + ToString(stored) +
        ", computed = " + ToString(computed) + "  in " + file_name +
        " offset " + ToString(offset) + " size " + ToString(block_size));
    // debug_buffer_mu.lock();
    // debug_buffer_ss << "[debug] " << "checksum mismatch, computed: " << computed << ", stored: " << stored << std::endl;
    // debug_buffer = debug_buffer_ss.rdbuf()->str().data();
    // debug_buffer_mu.unlock();
    DEBUG_STRUCT_SET(checksum_mismatch, true);
    // ROCKS_LOG_INFO(global_dboption->rubble_info_log, "checksum mismatch, computed: %u, stored: %u\n", computed, stored);
    // printf("[debug] checksum mismatch, computed: %u, stored: %u\n", computed, stored);
    // std::cout << "[debug] " << "checksum mismatch, computed: " << computed << ", stored: " << stored << std::endl;
  }
  DEBUG_STRUCT_SET(checksum_stored, stored);
  DEBUG_STRUCT_SET(checksum_computed, computed);
  
    /* char buffer[17];
    buffer[16] = 0;
    for(int j = 0; j < 8; ++j) {
      sprintf(&buffer[2*j], "%02hhX", *(data + j));
    }
    char endBuf[17];
    endBuf[16] = 0;
    const char* endOffset = data + block_size - 8;
    for(int j = 0; j < 8; ++j) {
      sprintf(&endBuf[2*j], "%02hhX", *(endOffset + j));
    }
    
    char blockBuff[2 * block_size+1];
    blockBuff[block_size] = 0;
    for(int j = 0; j < int(block_size); ++j) {
      sprintf(&blockBuff[2*j], "%02hhX", *(data + j));
    }
   
    std::cout << "stored: " << stored << ", computed: " << computed
	  << ", filename: " << file_name << ", offset: " << offset
          << ", [block]: " << blockBuff << "\n";
    */
  
  assert(s.ok());
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
