#include "util.h"
#include "rubble_server.h"

/* a server running a vanila rocksdb */
int main(int argc, char** argv) {
  //server is running on localhost:50051
  const std::string server_address = "localhost:50051";
  rocksdb::DB* db = GetDBInstance("/tmp/rocksdb_vanila_test","/mnt/sdb/archive_dbs/vanila/sst_dir", "" ,"", false, false, true);
  RunServer(db, server_address);
  return 0;
}
