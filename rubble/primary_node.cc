#include "util.h"
#include "rubble_async_server.h"

/**
 * primary/first node in the chain
 * 
 * for a 3-node setting, bring up a primary node, pass the secondary node's address
 * for a 2-node setting, pass the tail node's address
 */

int main(int argc, char** argv) {
  

  if(argc != 4){
    std::cout << "Usage: ./program primary_server_port secondary_addr shard_num \n";
    return 0;
  }

  const std::string primary_server_address = std::string("0.0.0.0:") +  argv[1];
  const std::string secondary_server_address = argv[2];
  const std::string remote_sst_dir = std::string("/mnt/remote-sst/") + argv[3];
  const std::string db_path = std::string("/mnt/db/") + argv[3] + "/primary/db";
  const std::string sst_path = std::string("/mnt/db/") + argv[3] + "/primary/sst_dir";
  rocksdb::DB* primary = GetDBInstance(db_path, sst_path, remote_sst_dir, 
    "", secondary_server_address, false, true, false);
  bool is_async = false;
  if(is_async){
    RunAsyncServer(primary, primary_server_address);
  }else{
    RunServer(primary, primary_server_address);
  }
  return 0;
}
