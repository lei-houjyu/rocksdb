#include "util.h"
#include "rubble_async_server.h"

/**
 * second node the chain 
 * default remote sst directory : /mnt/sdb/archive_dbs/tail/sst_dir
 */
int main(int argc, char** argv) {

  const std::string primary_server_address = "0.0.0.0:50053";
  if(argc != 2){
    std::cout << "Usage: ./program secondary_addr(example: 10.10.1.2:50050)\n";
    return 0;
  }

  // const std::string remote_sst_dir= "/mnt/nvme1n1p4/archive_dbs/tail/sst_dir";
  const std::string remote_sst_dir= "/mnt/sdb/archive_dbs/tail/sst_dir/";
  const std::string secondary_server_address= argv[1];
  const std::string db_path = "/mnt/sdb/archive_dbs/secondary/db";
  const std::string sst_path = "/mnt/sdb/archive_dbs/secondary/sst_dir";
  rocksdb::DB* primary = GetDBInstance(db_path, sst_path, remote_sst_dir, secondary_server_address, false, false, false);
  bool is_async = false;
  if(is_async){
    RunAsyncServer(primary, primary_server_address);
  }else{
    RunServer(primary, primary_server_address);
  }
  return 0;
}
