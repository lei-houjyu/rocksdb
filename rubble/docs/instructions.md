# Overview
This doc describes the steps to set up the environment and run a two-node experiment with colocation. First, let's get a glance at the high-level configuration. The replication topology that we are going to configure will be as follows:
* node-0: YCSB clients and the replicator
* node-1: primary A and secondary B
* node-2: primary B and secondary A

YCSB clients sends requests in batches to the replicator, which forwards them to the primary instance according to the key sharding. Between primary and secondary nodes, we adopt the Chain Replication protocol, i.e., writes go from head (primary) to tail (secondary), whereas reads are directly sent to the tail, and tail nodes return all the replies.

# Hardware Setup
## CloudLab Experiment
One can use this [profile](https://www.cloudlab.us/p/4d19d98c6e91426047ca6557b263c280a584ee38) to initialize an expriment on CloudLab, which consists of four [m510](http://docs.cloudlab.us/hardware.html) nodes. The usage of each node is as the above topology, except that we leave node-3 idle at this moment.

## NVMeoF Connection
Each m510 node has a 256GB NVMe device divided into four paritions, i.e., `/dev/nvme0n1p1` to `/dev/nvme0n1p4`. We use the last partition to build NVMeoF connections.

Execute the following commands on `node-1`. Keep in mind the `IP_ADDR` from the output, which we need to pass to the `client_setup.sh` in the next step.
```shell
node-1: wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/NVME_over_Fabrics/target_setup.sh
node-1: bash target_setup.sh
IP_ADDR: 10.10.1.2
Done!
```

On `node-2`, run
```shell
node-2: wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/NVME_over_Fabrics/client_setup.sh
node-2: bash client_setup.sh 10.10.1.2
```

As a sanity check, now you should be able to see a new device `nvme1n1p4` on node-2, which is node-1's device connected to node-2 via NVMeoF.
```shell
node-2: ls /dev/nvme* | grep nvme1n1p4
/dev/nvme1n1p4
```

Now, you have created an NVMeoF connection from node-2 to node-1. **Repeat the steps in reverse**, i.e., run `target_setup.sh` on node-2 and `client_setup.sh` on node-1 (notice that `IP_ADDR` is changed!), to build the connection from node-1 to node-2.

On both node-1 and node-2, we mount the local and remote device
```shell
node-1: mkdir /mnt/sdb; mount /dev/nvme0n1p4 /mnt/sdb
node-1: mkdir /mnt/remote; mount /dev/nvme1n1p4 /mnt/remote
```

# YCSB and Replicator Compilation
We set up these two components on node-0
```shell
node-0: git clone https://github.com/cc4351/YCSB.git
node-0: cd YCSB
node-0: git checkout single-thread
node-0: bash build.sh
```

# Rubble Compilation
Repeat the following steps on both node-1 and node-2
```shell
node-1: wget https://raw.githubusercontent.com/camelboat/EECS_6897_Distributed_Storage_System_Project_Scripts/rubble/setup_scripts/gRPC/grpc_setup.sh
node-1: bash grpc_setup.sh
node-1: sudo apt install libgflags-dev
node-1: export PATH=$PATH:/root/bin
node-1: cd /mnt/sdb
node-1: git clone https://github.com/camelboat/my_rocksdb.git
node-1: cd my_rocksdb
node-1: cmake .; make -j16; cd rubble; cmake .; make -j16
```

# Run the Experiment
Start priamry A and secondary B on node-1
```shell
node-1: mkdir -p /mnt/sdb/archive_dbs/primary/db
node-1: mkdir -p /mnt/sdb/archive_dbs/primary/sst_dir
node-1: mkdir -p /mnt/sdb/archive_dbs/tail/db
node-1: mkdir -p /mnt/sdb/archive_dbs/tail/sst_dir
node-1: cd /mnt/sdb/my_rocksdb/rubble
node-1: bash create_cgroups.sh
node-1: cgexec -g cpuset:rubble-cpu -g memory:rubble-mem ./primary_node 10.10.1.3:50052
node-1: cgexec -g cpuset:rubble-cpu -g memory:rubble-mem ./tail_node 10.10.1.1:50050 # in another terminal
```

Repeat the first six steps on node-2 and then start primary B and secondary A by
```shell
node-2: cgexec -g cpuset:rubble-cpu -g memory:rubble-mem ./primary_node 10.10.1.2:50052
node-2: cgexec -g cpuset:rubble-cpu -g memory:rubble-mem ./tail_node 10.10.1.1:50050 # in another terminal
```

The RocksDB configuration file is `rubble_16gb_config.ini` and `rubble_16gb_config_tail.ini`. You can change parameters like SST file size and Rubble mode.

Finally, we run the replicator and YCSB application by
```shell
node-0: ./bin/ycsb.sh replicator rocksdb -s -P workloads/workloada -p port=50050 -p shard=2 -p tail1=10.10.1.3:50052 -p head1=10.10.1.2:50051 -p tail2=10.10.1.2:50052 -p head2=10.10.1.3:50051
node-0: bash load.sh a localhost:50050 2 1000 10000 8 # in a different terminal
```
