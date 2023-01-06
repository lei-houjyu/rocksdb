#!/bin/bash

set -x

if [ $# -lt 5 ]; then
    echo "Usage: bash verify-sst-pool.sh target_file_size_base max_num_mems_in_flush pool_size shard_num rf recreate(0/1)"
    exit
fi

source helper.sh

target_file_size_base=$1
max_num_mems_in_flush=$2
pool_size=$3
shard_num=$4
rf=$5
recreate=$6
file_size=$(( target_file_size_base + 1048576 ))
local_nid=$( get_nid )
res="Clean"

check_sst_pool() {
    local dir=$1
    local size=$2
    local sst_num=`ls -l $dir | grep $size | wc -l`
    local outlier=`ls -l $dir | grep -vE "\.|txt|total|$size"`
    if [ -d $dir ] && [ $sst_num -eq $pool_size ] && [ $recreate -eq 0 ] && [ -z "$outlier" ]; then
        echo "OK"
    else
        echo "Bad"
    fi
}

for (( remote_nid=1; remote_nid<=$rf; remote_nid++ )); do
    if [ $remote_nid -ne $local_nid ]; then
        for (( sid=0; sid<$shard_num; sid++ )); do
            ret=$( is_head $remote_nid $sid $rf )
            if [ "$ret" == "true" ]; then
                shard_dir="/mnt/sst/node-${remote_nid}/shard-$sid"
                ret=$( check_sst_pool $shard_dir $file_size )
                if [ "$ret" == "Bad" ]; then
                    # 0. record error
                    res="Error"

                    # 1. umount the disk on remote host
                    remote_mount_point="/mnt/remote-sst/node-${local_nid}"
                    pid=$( nid_to_pid $remote_nid $local_nid )
                    nvme_id=$( nid_to_pid $local_nid $remote_nid )
                    nvme_id=$(( nvme_id - 1 ))
                    remote_dev="/dev/nvme${nvme_id}n1p${pid}"
                    ssh ${USER}@node-${remote_nid} "killall db_node; umount ${remote_mount_point}"

                    # 2. umount the disk locally
                    killall db_node
                    local_mount_point="/mnt/sst/node-${remote_nid}"
                    local_dev="/dev/nvme0n1p${pid}"
                    umount $local_mount_point
                    yes | mkfs.ext4 $local_dev
                    mount $local_dev $local_mount_point

                    # 3. recreate the SST pool
                    for (( i=0; i<$shard_num; i++ )); do
                        ret=$( is_head $remote_nid $i $rf )
                        if [ "$ret" == "true" ]; then
                            d="${local_mount_point}/shard-${i}"
                            bash create-sst-pool.sh $target_file_size_base $max_num_mems_in_flush $pool_size $d ${local_nid} ${i} > /dev/null 2>&1 &
                        fi
                    done
                    wait

                    # 4. remount the disk both locally and remotely
                    umount $local_mount_point
                    mount -o ro,noload $local_dev $local_mount_point
                    ssh ${USER}@node-${remote_nid} "mount ${remote_dev} ${remote_mount_point}"
                fi
                if [ $recreate -eq 1 ]; then
                    break
                fi
            fi
        done
    fi
done

echo $res