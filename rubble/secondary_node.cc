#include "rubble_server.h"

/**
 * second node the chain 
 * default remote sst directory : /mnt/sdb/archive_dbs/tail/sst_dir
 */
int main(int argc, char** argv) {

  if(argc != 2){
    std::cout << "usage: ./program tail_node_addr(example: 10.10.1.2:50049)\n";
    return 0;
  }

  //secondary db path
  std::string kDBPath = "/tmp/rubble_secondary";
  std::string secondary_server_address = "localhost:50050";

  rocksdb::DB* db;
  rocksdb::DBOptions db_options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  db_options.IncreaseParallelism();
  // create the DB if it's not already present
  db_options.create_if_missing = true;

  db_options.is_rubble = true;
  db_options.target_address = argv[1];
  db_options.remote_sst_dir = "/mnt/sdb/archive_dbs/tail/sst_dir";

  // secondary is really not doing flush sst to this dir, but in case of failure, secondary may needs to restart flush/compaction 
  // to recover to the previous state, so this dir is needed by the secondary
  db_options.db_paths.emplace_back(rocksdb::DbPath("/mnt/sdb/archive_dbs/secondary/sst_dir", 10000000000));
  
  rocksdb::ColumnFamilyOptions cf_options;
  cf_options.OptimizeLevelStyleCompaction();
  // Number of Level 5
  cf_options.num_levels=5;
  // L0 size 16MB
  cf_options.max_bytes_for_level_base=16777216;
  cf_options.compression=rocksdb::kNoCompression;
  // cf_options.compression_per_level=rocksdb::kNoCompression:kNoCompression:kNoCompression:kNoCompression:kNoCompression;

  const int kWriteBufferSize = 64*1024;
  // memtable size set to 4MB
  cf_options.write_buffer_size=kWriteBufferSize;

  // sst file size 4MB
  cf_options.target_file_size_base=4194304;
  cf_options.disable_auto_compactions=true;

  rocksdb::Options options(db_options, cf_options);
 
  rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
  
  //secondary server running on port 50050
  RunServer(db, secondary_server_address);
  return 0;
}
