#include "util.h"
#include "rubble_server.h"

/* a server running a vanila rocksdb */
int main(int argc, char** argv) {

  if (argc != 2) {
      std::cout << "Usage:./program --thread=xx";
      return 0;
  }

  int thread_num = std::atoi(ParseCmdPara(argv[1],"--thread="));
  //server is running on localhost:50051
  const std::string server_address = "localhost:50051";
  rocksdb::DB* db = GetDBInstance("/tmp/rocksdb_vanila_test","/mnt/sdb/archive_dbs/vanila/sst_dir", "" ,"", false, false, true);

  RunServer(db, server_address, thread_num);
  return 0;
}
