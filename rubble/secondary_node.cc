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
  rocksdb::DB* secondary = GetDBInstance("/tmp/rubble_secondary", "/mnt/sdb/archive_dbs/sst_dir","/mnt/remote/archive_dbs/sst_dir", tail_node_addr,true, false, false);
  
  //secondary server running on port 50050
  RunServer(secondary, "localhost:50050");
  return 0;
}
