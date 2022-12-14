#!/bin/bash

set -x

setup_as_target() {
    local offload=$1
    local ips=($`hostname -I`)
    local idx=$(( ${ips[1]::1} - 1))
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
    echo ${ips[1]} > /sys/kernel/config/nvmet/ports/1/addr_traddr
    echo "rdma" > /sys/kernel/config/nvmet/ports/1/addr_trtype
    echo "ipv4" > /sys/kernel/config/nvmet/ports/1/addr_adrfam

    ln -s /sys/kernel/config/nvmet/subsystems/${subsys}/ /sys/kernel/config/nvmet/ports/1/subsystems/${subsys}

    mount /dev/nvme0n1p1 /mnt/data
    mount -o ro,noload /dev/nvme0n1p2 /mnt/sst

    lsblk
}

setup_as_host() {
    local target_ip=$1
    local idx=$(( ${target_ip::1} - 1))
    local subsys='subsystem'$idx

    modprobe nvme-rdma

    nvme discover -t rdma -a $target_ip -s 4420
    nvme connect -t rdma -n testsubsystem -a $target_ip -s 4420
    nvme list

    lsblk
}

if [ $# -ne 2 ]
then
    echo "Usage: bash setup-nvmeof.sh target offload(0/1)"
    echo "Usage: bash setup-nvmeof.sh host successor-IP"
    exit
fi

if [ $1 == "target" ]
then
    setup_as_target $2
else
    setup_as_host $2
fi
