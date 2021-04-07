#!/bin/bash
# assumes sudo priviledge
mkdir -p /mnt/sdb && cd /mnt/sdb
git clone https://github.com/camelboat/my_rocksdb.git
cd my_rocksdb && git checkout chain && cd ..
wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/gRPC/grpc_setup.sh
wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/gRPC/cmake_install.sh
echo "ready to bash grpc_setup"
bash grpc_setup.sh
apt update && apt install libgflags-dev
cd /mnt/sdb/my_rocksdb/nlohmann_json/json
git clone https://github.com/nlohmann/json.git
mv json/* .
mkdir -p /mnt/sdb/archive_dbs/primary/sst_dir
mkdir -p /mnt/sdb/archive_dbs/tail/sst_dir
echo "end of script: remember to export PATH=/root/bin:$PATH"