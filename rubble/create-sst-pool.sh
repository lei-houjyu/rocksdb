#!/bin/bash

if [ $# != 6 ]; then
    echo "Usage: bash create-sst-pool.sh target_file_size_base max_num_mems_in_flush pool_size sst_dir nid sid"
    exit
fi

target_file_size_base=$1
max_num_mems_in_flush=$2
pool_size=$3
sst_dir=$4
nid=$5
sid=$6
padding=1048576

mkdir -p $sst_dir
cd $sst_dir
touch node-${nid}-shard-${sid}.txt

robust_create() {
    local fname=$1
    local fsize=$2
    local size=0

    while [ $size -ne $fsize ]; do
        head -c $fsize /dev/zero > $fname
        size=$( ls -l $fname | awk '{print $5}' )
    done
}

for i in $(seq 1 $pool_size); do
   target_file_size=`expr $target_file_size_base + $padding`
   echo $i
   if [ -f "$i" ] && [ `ls -l $i | awk '{print $5}'` == $target_file_size ] && [ "$(filefrag -e $i | grep unwritten)" == "" ]; then
       continue
   fi
   rm $i
   robust_create $i $target_file_size
done

n=`expr $pool_size + 1`
for t in $(seq 2 $max_num_mems_in_flush); do
    m=`expr $n + 99`
    target_file_size=`expr $target_file_size_base \* $t + $padding`
    for i in $(seq $n $m); do
        echo $i
        if [ -f "$i" ] && [ `ls -l $i | awk '{print $5}'` == $target_file_size ] && [ "$(filefrag -e $i | grep unwritten)" == "" ]; then
            continue
        fi
        rm $i
        robust_create $i $target_file_size
    done
    n=`expr $m + 1`
done
