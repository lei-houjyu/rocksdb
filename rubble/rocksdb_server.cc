#include "util.h"
#include "rubble_server.h"

/* a server running a vanila rocksdb */
int main(int argc, char** argv) {
  std::string target_addr = "";
  std::string rocksdb_dir = "";
  std::string sst_dir = "";
  std::cout << "argc : " << argc << std::endl;
  if(argc == 2){
    // std::cout << "Usage : ./program replicator's server address\n";
    rocksdb_dir = "/mnt/sdb/archive_dbs/vanilla/db";
    sst_dir = "/mnt/sdb/archive_dbs/vanilla/sst_dir";
    target_addr = argv[1];
  }else {
    rocksdb_dir = ParseCmdPara(argv[1], "--rocksdb_dir=");
    sst_dir = ParseCmdPara(argv[2], "--sst_dir=");
    target_addr = ParseCmdPara(argv[5],"--target_addr=");
    std::cout << "cmd : " << argv[5] << std::endl;
    std::cout << "target addr : " << target_addr << std::endl;
  }

  //server is running on localhost:50051
  const std::string server_address = "0.0.0.0:50051";

  rocksdb::DB* db = GetDBInstance(rocksdb_dir, sst_dir, "" , target_addr , false, false, true);
  RunServer(db, server_address);
  return 0;
}
