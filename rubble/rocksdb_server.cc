#include "util.h"
#include "rubble_server.h"

/* a server running a vanila rocksdb */
int main(int argc, char** argv) {
  std::string target_addr = "";
  if(argc == 2){
    std::cout << "Usage : ./program replicator's server address\n";
    target_addr = argv[1];
    // return 0; 
  }

  //server is running on localhost:50051
  const std::string server_address = "0.0.0.0:50051";
  rocksdb::DB* db = GetDBInstance("/tmp/rocksdb_vanila_test","/mnt/sdb/archive_dbs/vanila/sst_dir", "" , target_addr, false, false, true);
  RunServer(db, server_address);
  return 0;
}
