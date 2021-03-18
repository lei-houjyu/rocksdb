#include "util.h"
#include "rubble_server.h"

/* tail node in the chain */
int main(int argc, char** argv) {

//   if (argc != 2) {
//       std::cout << "Usage:./program --thread=xx";
//       return 0;
//   }

  //int thread_num = std::atoi(ParseCmdPara(argv[1],"--thread="));
  //std::string target_addr = std::atoi(ParseCmdPara(argv[2],"--target"));
  //server is running on localhost:50049;
  const std::string server_address = "localhost:50049"; 
  rocksdb::DB* db = GetDBInstance("/tmp/rubble_tail", "/mnt/sdb/archive_dbs/tail/sst_dir","","", true, false, true);

  RunServer(db, server_address /*, thread_num*/ );
  return 0;
}
