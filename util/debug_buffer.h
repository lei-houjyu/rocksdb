#pragma once

#include <sstream>
#include <mutex>
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

struct debug_struct_t {
    const char *key;
    const char *value;
    bool active_mem_hit;
    bool imm_mem_hit;
    bool mem_miss;
    uint64_t sst_file_number;   // TODO: make it a vector
    const char *table_cache_get_key;
    uint64_t data_block_handle_offset;
    uint64_t data_block_handle_size;
    bool data_block_cache_hit;
    bool direct_read;
    bool buffer_read;
    uint64_t read_file_offset;
    uint64_t read_file_size;
    uint64_t checksum_from_offset;
    uint64_t checksum_from_size;
    uint32_t checksum_stored;
    uint32_t checksum_computed;
    bool checksum_mismatch;
};

extern thread_local debug_struct_t debug_struct[10000];
extern thread_local int debug_struct_idx;

#define DEBUG_STRUCT_SET(attr, v)   \
            debug_struct[debug_struct_idx].attr = v

#define DEBUG_STRUCT_ADVANCE()      \
            debug_struct_idx = (debug_struct_idx + 1) % 10000;    \
            debug_struct[debug_struct_idx].key = nullptr;   \
            debug_struct[debug_struct_idx].active_mem_hit = false;  \
            debug_struct[debug_struct_idx].imm_mem_hit = false; \
            debug_struct[debug_struct_idx].mem_miss = false;    \
            debug_struct[debug_struct_idx].sst_file_number = 0; \
            debug_struct[debug_struct_idx].table_cache_get_key = nullptr;   \
            debug_struct[debug_struct_idx].data_block_handle_offset = 0;    \
            debug_struct[debug_struct_idx].data_block_handle_size = 0;  \
            debug_struct[debug_struct_idx].data_block_cache_hit = false;    \
            debug_struct[debug_struct_idx].direct_read = false;    \
            debug_struct[debug_struct_idx].buffer_read = false; \
            debug_struct[debug_struct_idx].read_file_offset = 0;    \
            debug_struct[debug_struct_idx].read_file_size = 0;  \
            debug_struct[debug_struct_idx].checksum_from_offset = 0;    \
            debug_struct[debug_struct_idx].checksum_from_size = 0;  \
            debug_struct[debug_struct_idx].checksum_stored = 0; \
            debug_struct[debug_struct_idx].checksum_computed = 0;   \
            debug_struct[debug_struct_idx].checksum_mismatch = false    \

// extern const ImmutableDBOptions *global_dboption;

// extern thread_local std::stringstream debug_buffer_ss;
// extern thread_local const char *debug_buffer;
// extern thread_local std::mutex debug_buffer_mu;
// static unsigned const MAX_DEBUG_BUFFER_SIZE = 10 * 1024; // 10 MB

// inline void ResetDebugBuffer() {
//     debug_buffer_mu.lock();
//     if (debug_buffer_ss.str().size() > MAX_DEBUG_BUFFER_SIZE) {
//         debug_buffer_ss.str(std::string());
//         debug_buffer_ss.clear();
//         debug_buffer_ss.str().reserve(2 * MAX_DEBUG_BUFFER_SIZE);
//     }
//     debug_buffer_mu.unlock();
// }
}
