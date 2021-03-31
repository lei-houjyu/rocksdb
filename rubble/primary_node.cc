#include "util.h"
#include "rubble_server.h"
/**
 * primary/first node in the chain
 * 
 * for a 3-node setting, bring up a primary node, pass the secondary node's address
 * for a 2-node setting, pass the tail node's address
 */

int main(int argc, char** argv) {
  
  const std::string primary_server_address = "localhost:50051";
  std::string target_address;
  if(argc == 2){
    target_address = argv[1];
    // std::cout << "Usage: ./program secondary_addr(example: 10.10.1.2:50050)\n";
    // return 0;
  }else if(argc == 1){
    target_address = "";
  }

  const std::string remote_sst_dir= "/mnt/remote/archive_dbs/sst_dir/";
  // secondary server is running on localhost:50050
  rocksdb::DB* primary = GetDBInstance("/tmp/rubble_primary", "/mnt/sdb/archive_dbs/sst_dir", remote_sst_dir, target_address,true, true, false);

  // primary server is running on localhost:50051
  RunServer(primary, primary_server_address);
  return 0;
}
