#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: bash save-result.sh shard_num suffix"
    exit
fi

shard_num=$1
suffix=$2

cp dstat.csv dstat-${suffix}.csv
cp iostat.out iostat-${suffix}.out
cp pids.out pids-${suffix}.out
cp top.out top-${suffix}.out
cp nethogs.out nethogs-${suffix}.out

for (( i=0; i<$shard_num; i++ ))
do
    cp shard-${i}.out shard-${i}-${suffix}.out
    cp /mnt/data/db/shard-${i}/db/LOG LOG-shard-${i}-${suffix}
done
