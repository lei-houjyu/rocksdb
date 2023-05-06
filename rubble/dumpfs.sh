#!/usr/bin/bash

suffix=$1
connected=$2

./dumpfs.py /dev/nvme0n1p2 /mnt/sst/node-2/shard-1/ local-node2-shard1-$suffix
./dumpfs.py /dev/nvme0n1p3 /mnt/sst/node-3/shard-2/ local-node3-shard2-$suffix

if [ $connected = 1 ]; then
    ./dumpfs.py /dev/nvme1n1p2 /mnt/remote-sst/node-2/shard-0/ remote-node2-shard0-$suffix
    ./dumpfs.py /dev/nvme2n1p2 /mnt/remote-sst/node-3/shard-0/ remote-node3-shard0-$suffix
fi
