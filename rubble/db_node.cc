#include "util.h"

int main(int argc, char** argv) {
    if(argc < 6) {
        std::cout << "Usage: ./db listen_port target_address shard_id replica_id replication_factor\n";
        return 0;
    }

    const std::string port = argv[1];
    const std::string addr = argv[2];
    const std::string sid  = argv[3];
    const int rid  = std::stoi(argv[4]);
    const int rf   = std::stoi(argv[5]);

    const std::string src_addr       = "0.0.0.0:" + port;
    const std::string dest_addr      = addr;
    const std::string db_path        = "/mnt/data/db/shard-" + sid + "/db";
    const std::string sst_path       = "/mnt/data/db/shard-" + sid + "/sst_dir";

    const bool is_rubble = false;
    const bool is_head   = (rid == 0);
    const bool is_tail   = (rid == rf - 1);

    // SST shipping address
    const std::string remote_sst_dir = is_tail ? "" : "/mnt/remote-sst/shard-" + sid;
    // SST pre-allocation address
    const std::string sst_pool_dir   = is_head ? "" : "/mnt/sst/shard-" + sid;

    rocksdb::DB* db = GetDBInstance(db_path, sst_path,
        remote_sst_dir, sst_pool_dir, dest_addr, is_rubble, is_head, is_tail, sid);

    RunServer(db, src_addr);

    return 0;
}
