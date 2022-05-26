#include "util.h"
#include "rubble_async_server.h"

/**
 * second node the chain 
 * default remote sst directory : /mnt/sdb/archive_dbs/tail/sst_dir
 */
int main(int argc, char** argv) {

  std::string target_addr = "";
  std::string server_port = "50151";
  std::string shard_num = "shard-0";
  std::string role_tag = "secondary-1";

  if (argc == 5) {
    server_port = argv[1];
    target_addr = argv[2];
    shard_num = argv[3];
    role_tag = argv[4];
  }

  const std::string server_addr = std::string("0.0.0.0:") + server_port;
  std::string db_path = std::string("/mnt/db/") + shard_num + "/" + role_tag + "/db";
  std::string sst_path = std::string("/mnt/db/") + shard_num + "/" + role_tag + "/sst_dir";
  std::string remote_sst_path = std::string("/mnt/remote-sst/") + shard_num;
  std::string sst_pool_path = std::string("/mnt/sst/") + shard_num; 
  rocksdb::DB* db = GetDBInstance(db_path, sst_path, remote_sst_path, sst_pool_path, 
    target_addr, false, false, false, shard_num);
  
  bool is_async = false;
  if(is_async){
    RunAsyncServer(db, server_addr);
  }else{
    RunServer(db, server_addr);
  }

  return 0;
}
