//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

#include <algorithm>
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include <unordered_map>
#include <iostream>
#include <chrono>
#include <mutex>

#include <stdio.h> 
#include <unistd.h> 
#include <stdlib.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string.h>

namespace ROCKSDB_NAMESPACE {

// conversion' conversion from 'type1' to 'type2', possible loss of data
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4244)
#endif
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

// maintain a view of secondary node's available sst slots
// sst_bit_map maintains a mapping between the sst number and occupied slots
// sst_bit_map[i] = j means sst_file with number j occupies the i-th slot where
// i is within range [1, 2, ..., preallocated_sst_pool_size]
// priamry node should update it when finish a flush/compaction
// secondary node will update it when received a Sync rpc call from the upstream node
static std::unordered_map<int, uint64_t> sst_bit_map;
static std::mutex mu;

// get an available sst slot
int GetAvailableSstSlot(int sst_pool_size, uint64_t sst_num){
  std::unique_lock<std::mutex> lk(mu);
  int sst_real = 0;
  int start = 1;
  int end = sst_pool_size;

  for(int i = start; i <= end; i++){
    if(sst_bit_map.find(i) == sst_bit_map.end()){
      // if not found, means slot is not occupied
      sst_real = i;
      break;
    }
  } 

  if(sst_real == 0){
    std::cerr << "run out of sst slot, slots taken : " << sst_bit_map.size() << std::endl;
    assert(sst_real != 0);
  }
  sst_bit_map.emplace(sst_real, sst_num);
  return sst_real;
}

// called when a file gets deleted in a compaction to free the slot and update the bitmap
void FreeSstSlot(uint64_t sst_num){
  std::unique_lock<std::mutex> lk(mu);
  auto it = sst_bit_map.begin();
  for(; it != sst_bit_map.end(); it++){
    if(it->second == sst_num){
    // if file gets deleted, free its occupied slot
      std::cout << " , free slot : " << it->first << std::endl;
      sst_bit_map.erase(it);
      break;
    }
  }
  assert(it != sst_bit_map.end());
}

// get the slot number taken by the sst file whose file number is sst_num
int GetTakenSlot(uint64_t sst_num){
  std::unique_lock<std::mutex> lk(mu);
  auto it = sst_bit_map.begin();
  for(; it != sst_bit_map.end(); it++){
    if(it->second == sst_num){
      return it->first;
    }
  }
  return 0;
}

int copy_sst(const std::string& from, const std::string& to, size_t size){
	int fd;
  char *buf = NULL;
	// 1. read primary's sst to buf
  // auto time_point_1 = std::chrono::high_resolution_clock::now();
  int ret = posix_memalign((void **)&buf, 512, size);
  // auto time_point_2 = std::chrono::high_resolution_clock::now();
  // std::cout << "Memalign time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_2 - time_point_1).count() << " microsecs\n";
  if (ret) {
    perror("posix_memalign failed");
    exit(1);
  }
  // memset(buf, 0, size);
  // auto time_point_3 = std::chrono::high_resolution_clock::now();
  // std::cout << "Memset time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_3 - time_point_2).count() << " microsecs\n";
  fd = open(from.c_str(), O_RDONLY | O_DIRECT, 0755);
  if (fd < 0) {
      perror("open sst failed");
      exit(1);
  }
  // auto time_point_4 = std::chrono::high_resolution_clock::now();
  // std::cout << "open file time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_4 - time_point_3).count() << " microsecs\n";
 
	ret = read(fd, buf, size);
  // auto time_point_5 = std::chrono::high_resolution_clock::now();
  // std::cout << "read file time : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_5 - time_point_4).count() << " microsecs\n";
	if (ret < 0) {
		perror("read sst failed");
	}
  close(fd);

  // Slice s(&buf[size - 53] , 53);
  // std::cout << "size : " << size <<  ", magic number : " << s.ToString() << std::endl;
     
  // 2. write buf to secondary's sst   
  // printf("write to %s\n", to.c_str());
  fd = open(to.c_str(), O_WRONLY | O_DIRECT , 0755);
  if (fd < 0){
      perror("open sst failed");
      exit(1);
  }

  struct stat stat_buf;
  fstat(fd, &stat_buf);
  size_t f_size = static_cast<size_t>(stat_buf.st_size);
  assert(f_size == size);

  // auto time_point_6 = std::chrono::high_resolution_clock::now();
	ret = write(fd, buf, size);
  // auto time_point_7 = std::chrono::high_resolution_clock::now();
	if (ret < 0) {
		perror("write sst failed");
	}

  // std::cout << "wrote " << ret << " bytes to " << to << " ,latency : " << std::chrono::duration_cast<std::chrono::microseconds>(time_point_7 - time_point_6).count() << " microsecs\n";
  close(fd);
  free(buf);
	return 0;
}

}  // namespace ROCKSDB_NAMESPACE
