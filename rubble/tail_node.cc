#include "util.h"
#include "rubble_server.h"

/* tail node in the chain */
int main(int argc, char** argv) {
  std::string target_addr = "";
  if (argc == 2) {
      target_addr = argv[1];
  }
  const std::string server_addr = "0.0.0.0:50052"; 
  rocksdb::DB* db = GetDBInstance("/tmp/rubble_tail", "/mnt/sdb/archive_dbs/tail/sst_dir","", target_addr, false, false, true);

  RunServer(db, server_addr, 16);
  return 0;
}
