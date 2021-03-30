#pragma once

#include <string>
#include <iostream>
#include <cstring>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
using std::string;

const char* ParseCmdPara( char* argv,const char* para) {
    auto p_target = std::strstr(argv,para);
    if (p_target == nullptr) {
        printf("para error argv[%s] should be %s \n",argv,para);
        return nullptr;
    }
    p_target += std::strlen(para);
    return p_target;
}

/**
 * @param db_path the db path
 * @param sst_dir local sst directory
 * @param remote_sst_dir not "" for non-tail nodes
 * @param target_addr the downstream node's address in the chain, for the non-tail nodes, it's forwarding ops to this address, 
 *                    for the tail node, this target_addr is replicator's address to which it's sending the true reply.
 * @param is_rubble when enabled, compaction is disabled when is_primary sets to false,
 *                   when disabled, all nodes are running flush/compaction
 * @param is_primary set to true for first/primary node in the chain 
 * @param is_tail    set to true for tail node in the chain
 */
rocksdb::DB* GetDBInstance(const string& db_path, const string& sst_dir, 
                                const string& remote_sst_dir,
                                const string& target_addr, 
                                bool is_rubble, bool is_primary, bool is_tail){

  rocksdb::DB* db;
  rocksdb::DBOptions db_options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  db_options.IncreaseParallelism();
  // create the DB if it's not already present
  db_options.create_if_missing = true;

  db_options.is_rubble=is_rubble;
  db_options.is_primary=is_primary;
  db_options.is_tail=is_tail;
  db_options.target_address=target_addr;

    // for non-tail nodes in rubble mode, it's shipping sst file to the remote_sst_dir;
    if(is_rubble && !is_tail){
        db_options.remote_sst_dir=remote_sst_dir;
    }

  db_options.db_paths.emplace_back(rocksdb::DbPath(sst_dir, 10000000000));
  
  rocksdb::ColumnFamilyOptions cf_options;
  cf_options.OptimizeLevelStyleCompaction();
  cf_options.num_levels=5;

  // L0 size 16MB
  cf_options.max_bytes_for_level_base=16777216;
  cf_options.compression=rocksdb::kNoCompression;
  // cf_options.compression_per_level=rocksdb::kNoCompression:kNoCompression:kNoCompression:kNoCompression:kNoCompression;

  const int kWriteBufferSize = 4*(1<<20);
  // memtable size set to 4MB
  cf_options.write_buffer_size=kWriteBufferSize;
  // sst file size 4MB
  cf_options.target_file_size_base=4194304;
 
  rocksdb::Options options(db_options, cf_options);

  // open DB
  rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db);
  if(!s.ok()){
    std::cout << "DB open failed : " << s.ToString() << std::endl;
  }
  assert(s.ok());

  return db;
}

