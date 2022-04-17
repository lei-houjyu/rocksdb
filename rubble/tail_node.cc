#include "util.h"
#include "rubble_async_server.h"

/* tail node in the chain */
int main(int argc, char** argv) {
  std::string target_addr = "";
  if (argc == 2) {
      target_addr = argv[1];
  }
  const std::string server_addr = "0.0.0.0:50052";
  std::string db_path = "/mnt/db/tail/db";
  std::string sst_path = "/mnt/db/tail/sst_dir";
  std::string sst_pool_path = "/mnt/sst"; 
  rocksdb::DB* db = GetDBInstance(db_path, sst_path, "", sst_pool_path, target_addr, false, false, true);
  
  bool is_async = false;
  if(is_async){
    RunAsyncServer(db, server_addr);
  }else{
    RunServer(db, server_addr);
  }

  return 0;
}
