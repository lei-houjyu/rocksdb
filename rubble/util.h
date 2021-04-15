#pragma once

#include <string>
#include <iostream>
#include <cstring>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"

using std::string;
using rocksdb::LoadOptionsFromFile;

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
  rocksdb::ConfigOptions config_options;
  std::vector<rocksdb::ColumnFamilyDescriptor> loaded_cf_descs;
  rocksdb::Status s = LoadOptionsFromFile(
    /*config_options=*/config_options,
    /*options_file_name=*/"/mnt/sdb/my_rocksdb/rubble/rocksdb_config_file.ini",
    /*db_options=*/&db_options,
    /*cf_descs=*/&loaded_cf_descs
  );
  if (!s.ok()) std::cout << s.getState() << '\n';
  assert(s.ok());
  std::cout << "is_rubble: " << db_options.is_rubble << '\n';
  std::cout << "is_primary: " << db_options.is_primary << '\n';
  std::cout << "is_tail: " << db_options.is_tail << '\n';
  std::cout << "number of cf: " << loaded_cf_descs.size() << '\n';
  // for (auto &desc : loaded_cf_descs) {
  //   std::cout << "cf name: " << desc.name << '\n';
  //   std::cout << "write_buffer_size: " << desc.options.write_buffer_size << '\n';
  //   std::cout << "target_file_size_base: " << desc.options.target_file_size_base << '\n';
  // }
  assert(loaded_cf_descs.size() == 1); // We currently only support one ColumnFamily
 
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  db_options.IncreaseParallelism();
  
  db_options.target_address=target_addr; //TODO(add target_addr, remote_sst_dir and preallocated_sst_pool_size to option file)

  // for non-tail nodes in rubble mode, it's shipping sst file to the remote_sst_dir;
  if(is_rubble && !is_tail){
      db_options.remote_sst_dir=remote_sst_dir;
  }
  
  uint64_t target_size = 10000000000;
  db_options.db_paths.emplace_back(rocksdb::DbPath(sst_dir, 10000000000));
  
  rocksdb::ColumnFamilyOptions cf_options = loaded_cf_descs[0].options;

  if(is_rubble && !is_primary){
    // db_options.use_direct_reads = true;
    // right now, just set sst pool size to 100 if it's sufficient
    // db_options.preallocated_sst_pool_size = db_options.db_paths.front().target_size / (((cf_options.write_buffer_size >> 20) + 1) << 20);
    db_options.preallocated_sst_pool_size = 100;
  }

  std::cout << "write_buffer_size: " << cf_options.write_buffer_size << '\n';
  std::cout << "target_file_size_base: " << cf_options.target_file_size_base << '\n';
  rocksdb::Options options(db_options, cf_options);

  // open DB
  // rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db);
  s = rocksdb::DB::Open(options, db_path, &db);
  if(!s.ok()){
    std::cout << "DB open failed : " << s.ToString() << std::endl;
  }
  assert(s.ok());

  return db;
}

