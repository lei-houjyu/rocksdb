#include "util.h"
#include "rubble_async_server.h"

/* tail node in the chain */
int main(int argc, char** argv) {
  std::string target_addr = "";
  std::string server_port = "50052";
  std::string shard_num = "shard-0";

  if (argc == 4) {
    server_port = argv[1];
    target_addr = argv[2];
    shard_num = argv[3];
  }
  const std::string server_addr = std::string("0.0.0.0:") + server_port;
  std::string db_path = std::string("/mnt/db/") + shard_num + "/tail/db";
  std::string sst_path = std::string("/mnt/db/") + shard_num + "/tail/sst_dir";
  std::string sst_pool_path = std::string("/mnt/sst/") + shard_num; 
  rocksdb::DB* db = GetDBInstance(db_path, sst_path, "", sst_pool_path, 
    target_addr, false, false, true, shard_num);
  
  bool is_async = false;
  if(is_async){
    RunAsyncServer(db, server_addr);
  }else{
    RunServer(db, server_addr);
  }

  return 0;
}
