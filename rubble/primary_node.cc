#include "util.h"
#include "rubble_async_server.h"

/**
 * primary/first node in the chain
 * 
 * for a 3-node setting, bring up a primary node, pass the secondary node's address
 * for a 2-node setting, pass the tail node's address
 */

int main(int argc, char** argv) {
  
  const std::string primary_server_address = "0.0.0.0:50051";
  if(argc != 2){
    std::cout << "Usage: ./program secondary_addr(example: 10.10.1.2:50050)\n";
    return 0;
  }

  // const std::string remote_sst_dir= "/mnt/nvme1n1p4/archive_dbs/tail/sst_dir";
  const std::string remote_sst_dir= "/mnt/sdb/archive_dbs/secondary/sst_dir/";
  const std::string secondary_server_address= argv[1];
  const std::string db_path = "/mnt/sdb/archive_dbs/primary/db";
  const std::string sst_path = "/mnt/sdb/archive_dbs/primary/sst_dir";
  rocksdb::DB* primary = GetDBInstance(db_path, sst_path, remote_sst_dir, secondary_server_address, false, true, false);
  bool is_async = false;
  if(is_async){
    RunAsyncServer(primary, primary_server_address);
  }else{
    RunServer(primary, primary_server_address);
  }
  return 0;
}
