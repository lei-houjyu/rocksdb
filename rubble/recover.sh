#!/bin/bash

if [ $# != 1 ]; then
  echo "Usage: bash recover.sh shard_num"
fi

g++ -o ship_sst test/write_direct_test.cc -D_GNU_SOURCE

rm -rf /mnt/db

cp -r /mnt/backup/db /mnt/db

# Re-ship SST files to remote secondaries
for i in $(seq $1); do
    log_fname="/mnt/backup/db/$i/primary/db/LOG"
    for local in /mnt/db/$i/primary/sst_dir/*; do
    {
        remote=`grep "local_fname: $local" $log_fname | awk '{print $(NF)}'`
        echo $local $remote
        ./ship_sst $local $remote
    }&
    done
done

wait

