#!/usr/bin/bash

if [ $# != 2 ]; then
    echo "Usage: ./mark-load-end.sh shards_num suffix"
fi

shards=$1
suffix=$2

for i in $(seq 0 $((${shards}-1))); do
    sst_cnt=`grep "Shipped SST" /mnt/data/db/shard-${i}/db/LOG | tail -1 | grep -o 'total count: [0-9]*' | cut -d' ' -f3`
    if [ -z ${sst_cnt} ]; then
        sst_cnt=0
    fi
    echo "shard ${i},${sst_cnt}" >> LOAD_PHASE_SST_MARK_${suffix}.out
done
