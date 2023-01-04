#!/bin/bash

if [ $# -lt 5 ]; then
    echo "Usage: bash verify-sst-pool.sh target_file_size_base max_num_mems_in_flush pool_size shard_num rf"
    exit
fi

source helper.sh

local target_file_size_base=$1
local max_num_mems_in_flush=$2
local pool_size=$3
local shard_num=$4
local rf=$5
local file_size=$(( target_file_size_base + 1048576 ))
local local_nid=$( get_nid )
local res="Clean"

for (( remote_nid=1; remote_nid<=$rf; remote_nid++ )); do
    if [ $remote_nid -ne $local_nid ]; then
        for (( sid=0; sid<$shard_num; sid++ )); do
            local ret=$( is_head $remote_nid $sid $rf )
            if [ "$ret" == "true" ]; then
                local shard_dir="/mnt/sst/node-${remote_nid}/shard-$sid"
                ret=$( check_sst_pool $shard_dir $file_size )
                if [ "$ret" == "Bad" ]; then
                    # 0. record error
                    res="Error"

                    # 1. umount the disk on remote host
                    local remote_mount_point="/mnt/remote-sst/node-${local_nid}"
                    local remote_dev=$( ssh ${USER}@node-${remote_nid} "df -h | grep ${remote_mount_point} | awk '{print \$1}'" )
                    ssh ${USER}@node-${remote_nid} "killall db_node; umount ${remote_mount_point}"

                    # 2. umount the disk locally
                    killall db_node
                    local local_mount_point="/mnt/sst/node-${remote_nid}"
                    local local_dev=$( df -h | grep ${local_mount_point} | awk '{print $1}' )
                    umount $local_mount_point
                    yes | mkfs.ext4 $local_dev
                    mount $local_dev $local_mount_point

                    # 3. recreate the SST pool
                    for (( i=0; i<$shard_num; i++ )); do
                        ret=$( is_head $remote_nid $i $rf )
                        if [ "$ret" == "true" ]; then
                            local d="${local_mount_point}/shard-${i}"
                            bash create-sst-pool.sh $target_file_size_base $max_num_mems_in_flush $pool_size $d > /dev/null 2>&1 &
                        fi
                    done
                    wait

                    # 4. remount the disk both locally and remotely
                    umount $local_mount_point
                    mount -o ro,noload $local_dev $local_mount_point
                    ssh ${USER}@node-${remote_nid} "mount ${remote_dev} ${remote_mount_point}"
                fi
            fi
        done
    fi
done

echo $res