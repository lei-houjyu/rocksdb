#!/bin/bash

# Parameter:
#   - node ID
#   - shard ID
#   - replication factor
# Return value:
#   - true, if node-$nid holds the primary of shard-$sid
#   - false, otherwise
is_head() {
    local nid=$1
    local sid=$2
    local rf=$3

    local idx=$(( nid - 1 ))
    if [ $(($sid % $rf)) -eq $idx ]; then
        echo "true"
    else
        echo "false"
    fi
}

# Parameter:
#   - node ID
#   - shard ID
#   - replication factor
# Return value:
#   - partition number x, so that /dev/nvme0n1p$x will be mount to /mnt/sst/shard-$sid on node-$nid
sid_to_pid() {
    local nid=$1
    local sid=$2
    local rf=$3

    local pid=2
    for (( i=0; i<$sid; i++ )); do
        ret=$( is_head $nid $i $rf )
        if [ "$ret" == "false" ]; then
            pid=$(( pid + 1 ))
        fi
    done

    echo $pid
}

# Parameter:
#   - local node ID a
#   - remote node ID b
# Return value:
#   - partition number x, so that /dev/nvme0n1p$x on node-b holds SST files from node-a
nid_to_pid() {
    local local_nid=$1
    local remote_nid=$2

    if [ $local_nid -lt $remote_nid ]; then
        pid=$(( local_nid + 1 ))
    else
        pid=$local_nid
    fi

    echo $pid
}

# Parameter:
#   - shard ID
#   - replication factor
# Return value:
#   - node ID x, so that node-x holds the primary of shard ID
sid_to_nid() {
    local sid=$1
    local rf=$2

    local nid=$(( sid % rf + 1 ))

    echo $nid 
}

# Parameter:
#   - private IP
# Return value:
#   - node ID, so that node-$nid's private IP is $ip
ip_to_nid() {
    local ip=$1
    local digit=${ip: -1}
    local nid=$(( digit - 1 ))
    echo $nid
}

# Parameter:
#   None
# Return value:
#   - node ID of the current server
get_nid() {
    local ip=`hostname -I | awk '{print $2}'`
    local nid=$( ip_to_nid $ip )
    echo $nid
}

# Parameter:
#   - replication factor
#   - mount option
# Return value:
#   None
mount_local_disk() {
    local rf=$1
    local option=$2
    local self=$( get_nid )
    local pid=2

    mount /dev/nvme0n1p1 /mnt/data
    for (( nid=1; nid<=$rf; nid++ )); do
        if [ $nid -ne $self ]; then
            mount ${option} /dev/nvme0n1p${pid} /mnt/sst/node-${nid}
            pid=$(( pid + 1 ))
        fi
    done

    lsblk
}