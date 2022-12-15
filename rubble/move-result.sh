#!/bin/bash
#for cf in {1..2}; do for rf in {2..4}; do sn=$(( cf * rf ));   for mode in 'baseline' 'rubble'; do for workload in 'load' 'a' 'b' 'c' 'd'; do bash move-result.sh /mnt/data/my_rocksdb/rubble/results/r6525/$sn-shard-$rf-replica $mode-$workload-$sn-shard-$rf-replica $sn $rf; done; done; done; done

if [ $# -lt 4 ]; then
    echo "Usage: bash move-result.sh abs_dest suffix shard_num rf"
    exit
fi

dir=$1
suffix=$2
shard_num=$3
rf=$4
rubble_path='/mnt/data/my_rocksdb/rubble/'

for (( i=1; i<=$rf; i++ ))
do
    svr='10.10.1.'$(( i + 1 ))
    dest=${dir}/node-${i}
    mkdir -p $dest
    
    for prefix in 'dstat' 'iostat' 'pids' 'top'
    do
        scp ${USER}@${svr}:${rubble_path}${prefix}'-'${suffix}* ${dest}
    done
    
    for (( s=0; s<$shard_num; s++ ))
    do
        scp ${USER}@${svr}:${rubble_path}'shard-'${s}'-'${suffix}'.out' ${dest}
        scp ${USER}@${svr}:${rubble_path}'LOG-shard-'${s}'-'${suffix} ${dest}
    done
done

