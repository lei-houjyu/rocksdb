#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: bash init-sst-pool.sh shard_num replication_factor"
    exit
fi

shard_num=$1
rf=$2

umount /mnt/sst /mnt/