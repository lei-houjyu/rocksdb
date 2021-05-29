#include "util.h"
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

  std::string tail_node_addr = argv[1];
  const std::string db_path = "/mnt/sdb/archive_dbs/secondary/db";
  const std::string sst_path = "/mnt/sdb/archive_dbs/secondary/sst_dir";
  const std::string remote_sst_dir = "/mnt/nvme1n1p4/archive_dbs/tail/sst_dir";
  rocksdb::DB* secondary = GetDBInstance(db_path, sst_path,remote_sst_dir, tail_node_addr,true, false, false);
  
  //secondary server running on port 50051
  RunServer(secondary, "0.0.0.0:50051");
  return 0;
}
