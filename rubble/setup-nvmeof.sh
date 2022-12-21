#!/bin/bash

set -x

mount_local_disk() {
    mount /dev/nvme0n1p1 /mnt/data
    local pid=2
    for dir in `ls /mnt/sst`
    do
        mount -o ro,noload /dev/nvme0n1p$pid /mnt/sst/$dir
        pid=$(( pid + 1 ))
    done

    lsblk
}

mount_remote_disk() {
    local remote_nid=$1
    local nvme_id=$2
    local shard_num=$3
    local rf=$4

    local private_ip=`hostname -I | awk '{print $2}'`
    local local_nid=$( ip_to_nid $private_ip )

    for (( sid=0; sid<$shard_num; sid++ ))
    do
        local val=$( is_head $local_nid $sid $rf )
        if [ "$val" == "true" ]
        then
            local pid=$( sid_to_pid $remote_nid $sid $rf )
            local mount_point=/mnt/remote-sst/node-${remote-nid}/shard-${sid}
            mkdir -p $mount_point
            mount /dev/nvme${nvme_id}n1p${pid} $mount_point
        fi
    done
}

setup_as_target() {
    local offload=$1
    local ips=($`hostname -I`)
    local private_ip=${ips[1]}
    local idx=$(( ${private_ip: -1} - 1 ))
    local subsys='subsystem'$idx

    lsblk

    /usr/local/etc/emulab/rc/rc.ifconfig shutdown
    /usr/local/etc/emulab/rc/rc.ifconfig boot

    if [ $offload -eq 1 ]; then
        modprobe -r nvme
        modprobe nvme num_p2p_queues=2
    fi
    modprobe nvmet
    modprobe nvmet-rdma

    mkdir /sys/kernel/config/nvmet/subsystems/${subsys}

    echo 1 > /sys/kernel/config/nvmet/subsystems/${subsys}/attr_allow_any_host
    
    if [ $offload -eq 1 ]; then
        echo 1 > /sys/kernel/config/nvmet/subsystems/${subsys}/attr_offload
    fi

    mkdir /sys/kernel/config/nvmet/subsystems/${subsys}/namespaces/1

    echo -n /dev/nvme0n1 > /sys/kernel/config/nvmet/subsystems/${subsys}/namespaces/1/device_path
    sleep 5
    echo 1 > /sys/kernel/config/nvmet/subsystems/${subsys}/namespaces/1/enable

    mkdir /sys/kernel/config/nvmet/ports/1

    echo 4420 > /sys/kernel/config/nvmet/ports/1/addr_trsvcid
    echo $private_ip > /sys/kernel/config/nvmet/ports/1/addr_traddr
    echo "rdma" > /sys/kernel/config/nvmet/ports/1/addr_trtype
    echo "ipv4" > /sys/kernel/config/nvmet/ports/1/addr_adrfam

    ln -s /sys/kernel/config/nvmet/subsystems/${subsys}/ /sys/kernel/config/nvmet/ports/1/subsystems/${subsys}

    mount_local_disk
}

setup_as_host() {
    local target_ip=$1
    local shard_num=$2
    local rf=$3
    local nvme_id=$4
    local nid=$( ip_to_nid $target_ip )
    local subsys='subsystem'$nid
    local before=`nvme list`

    modprobe nvme-rdma

    nvme discover -t rdma -a $target_ip -s 4420
    nvme connect -t rdma -n $subsys -a $target_ip -s 4420
    while [ "$(nvme list)" == "$before" ]
    do
        sleep 1
    done

    mount_remote_disk $nid $nvme_id $shard_num $rf

    lsblk
}

if [ $# -lt 2 ]
then
    echo "Usage: bash setup-nvmeof.sh target offload(0/1)"
    echo "Usage: bash setup-nvmeof.sh host target-IP shard_num rf nvme_id"
    exit
fi

source /root/helper.sh

if [ $1 == "target" ]
then
    setup_as_target $2
else
    setup_as_host $2 $3 $4 $5
fi
