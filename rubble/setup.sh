#!/bin/bash

set -x

if [ $# -lt 4 ]; then
    echo "Usage: bash setup.sh username IP-1 IP-2 IP-3 ..."
    exit
fi

ssh_arg="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

check_connectivity() {
    status=$1
    shift 1
    for ip in $@
    do
        echo $ip
        while true
        do
            ssh $ssh_arg -q $username@$ip exit
            if [ $? -eq $status ]
            then
                break 1
            fi
        done
    done
}

username=$1
shift 1

# Step 1: configure SSH keys on each node
for ip in $@;
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/setup-keys.sh; sudo bash setup-keys.sh;"
done

# Step 2: set up YCSB benchmark on IP-1
ycsb_node=$1
shift 1
ssh $ssh_arg $username@$ycsb_node "sudo apt update; yes | sudo apt install maven"
ssh $ssh_arg $username@$ycsb_node "git clone --branch single-thread https://github.com/cc4351/YCSB.git; cd YCSB; nohup bash build.sh > ycsb_build.log 2>&1 &"

# Step 3: set up Rubble from IP-2 to IP-3
rubble_node=$@
shard_num=$#

for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/setup-rubble.sh; nohup sudo bash setup-rubble.sh > rubble_build.log 2>&1 &"
done

# Step 4: set up NVMeoF
# Step 4a: disable iommu and reboot each node
for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/disable-iommu.sh; sudo bash disable-iommu.sh"
done
check_connectivity 0 $rubble_node

# Step 4b: install MLNX_OFED driver and reboot
for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/install-mlnx-ofed.sh; nohup sudo bash install-mlnx-ofed.sh > mlnx_install.log 2>&1 &"
done
check_connectivity 255 $rubble_node
check_connectivity 0   $rubble_node

# Step 4c: each node nvme-connects to its successor
for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/setup-nvmeof.sh; sudo bash setup-nvmeof.sh target > nvmeof.log 2>&1"
done

for (( i=0; i<$shard_num; i++))
do
    j=$((($i+1)%$shard_num))
    ip=${rubble_node[$i]}
    next_ip=${rubble_node[$j]}
    ssh $ssh_arg $username@$ip "sudo bash setup-nvmeof.sh host ${next_ip} >> nvmeof.log 2>&1"
done

echo "Done!"
