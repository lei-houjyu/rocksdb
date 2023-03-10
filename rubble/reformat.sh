#!/bin/bash

set -x

source /root/helper.sh

nvme_dev='/dev/nvme0n1'
DATA_PATH="/mnt/data"
SST_PATH="/mnt/sst"

shard_num=$1
rf=$2

mount_local_disk $rf ""
lsblk

nid=$( get_nid )
for (( sid=0; sid<${shard_num}; sid++ ));
do
    for f in db sst_dir;
    do
        mkdir -p ${DATA_PATH}/db/shard-${sid}/${f} 
    done
    ret=$( is_head $nid $sid $rf )
    if [ "$ret" == "false" ]
    then
        primary_node=$( sid_to_nid $sid $rf )
        shard_dir=${SST_PATH}/node-${primary_node}/shard-${sid}
        bash create-sst-pool.sh 16777216 1 1448 ${shard_dir} ${nid} ${sid} > /dev/null 2>&1 &
    fi
done
wait

for dev in `ls ${nvme_dev}p*`
do
    umount $dev
done

lsblk
