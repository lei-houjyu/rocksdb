#include "rubble_server.h"

/* tail node in the chain */
rocksdb::DB* GetDBInstance(const std::string& db_path){

  rocksdb::DB* db;
  rocksdb::DBOptions db_options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  db_options.IncreaseParallelism();
  // create the DB if it's not already present
  db_options.create_if_missing = true;
  db_options.is_rubble=true;
  db_options.is_tail=true;

  db_options.db_paths.emplace_back(rocksdb::DbPath("/mnt/sdb/archive_dbs/tail/sst_dir", 10000000000));
  
  rocksdb::ColumnFamilyOptions cf_options;
  cf_options.OptimizeLevelStyleCompaction();
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
 
  rocksdb::Options options(db_options, cf_options);

  // open DB
  rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db);
  assert(s.ok());

  return db;
}

int main(int argc, char** argv) {

//   if (argc != 2) {
//       std::cout << "Usage:./program --thread=xx";
//       return 0;
//   }

  //int thread_num = std::atoi(ParseCmdPara(argv[1],"--thread="));
  //std::string target_addr = std::atoi(ParseCmdPara(argv[2],"--target"));
  //server is running on localhost:50049;
  const std::string server_address = "localhost:50049"; 
  rocksdb::DB* db = GetDBInstance("/tmp/rubble_tail");

  RunServer(db, server_address /*, thread_num*/ );
  return 0;
}
