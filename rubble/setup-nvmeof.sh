#!/bin/bash

set -x

setup_as_target() {
    /etc/init.d/openibd restart

    /usr/local/etc/emulab/rc/rc.ifconfig shutdown
    /usr/local/etc/emulab/rc/rc.ifconfig boot

    modprobe -r nvme
    modprobe nvme num_p2p_queues=2
    modprobe nvmet
    modprobe nvmet-rdma

    mkdir /sys/kernel/config/nvmet/subsystems/testsubsystem

    echo 1 > /sys/kernel/config/nvmet/subsystems/testsubsystem/attr_allow_any_host
    echo 1 > /sys/kernel/config/nvmet/subsystems/testsubsystem/attr_offload

    mkdir /sys/kernel/config/nvmet/subsystems/testsubsystem/namespaces/1

    echo -n /dev/nvme0n1 > /sys/kernel/config/nvmet/subsystems/testsubsystem/namespaces/1/device_path
    sleep 5
    echo 1 > /sys/kernel/config/nvmet/subsystems/testsubsystem/namespaces/1/enable

    mkdir /sys/kernel/config/nvmet/ports/1

    ips=($`hostname -I`)
    echo 4420 > /sys/kernel/config/nvmet/ports/1/addr_trsvcid
    echo ${ips[1]} > /sys/kernel/config/nvmet/ports/1/addr_traddr
    echo "rdma" > /sys/kernel/config/nvmet/ports/1/addr_trtype
    echo "ipv4" > /sys/kernel/config/nvmet/ports/1/addr_adrfam

    ln -s /sys/kernel/config/nvmet/subsystems/testsubsystem/ /sys/kernel/config/nvmet/ports/1/subsystems/testsubsystem
}

setup_as_host() {
    target_ip=$1
    modprobe nvme-rdma
    nvme discover -t rdma -a $target_ip -s 4420
    nvme connect -t rdma -n testsubsystem -a $target_ip -s 4420
    nvme list
    mkdir /mnt/remote-sst
    mount /dev/nvme1n1p2 /mnt/remote-sst
}

if [ $# -lt 1 ]
then
    echo "Usage: bash setup-nvmeof.sh target"
    echo "Usage: bash setup-nvmeof.sh host successor-IP"
    exit
fi

if [ $1 == "target" ]
then
    setup_as_target
else
    setup_as_host $2
fi

