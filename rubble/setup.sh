#!/bin/bash

set -x

if [ $# -lt 4 ]; then
    echo "Usage: bash setup.sh username IP-1 IP-2 IP-3 ..."
    exit
fi

ssh_arg="-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
ssh_down=255
ssh_up=0

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
log=">> key_setup.log 2>&1"
for ip in $@
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/setup-keys.sh ${log}; sudo bash setup-keys.sh ${log}" &
done
wait

# Step 2: set up YCSB benchmark on IP-1
log=">> ycsb_build.log 2>&1"
ycsb_node=$1
shift 1
ssh $ssh_arg $username@$ycsb_node "sudo apt update ${log}; yes | sudo apt install maven python3-pip ${log}; sudo pip3 install matplotlib ${log}"
ssh $ssh_arg $username@$ycsb_node "git clone --branch single-thread https://github.com/cc4351/YCSB.git ${log}; cd YCSB; nohup bash build.sh ${log} &"

# Step 3: set up Rubble from IP-2 to IP-3
log=">> rubble_build.log 2>&1"
rubble_node=$@
shard_num=$#

for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/setup-rubble.sh ${log}; sudo bash setup-rubble.sh ${log}" &
done
wait

# Step 4: set up NVMeoF
# Step 4a: disable iommu and reboot each node
log=">> iommu.log 2>&1"
for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/disable-iommu.sh ${log}; sudo bash disable-iommu.sh ${log}" &
done
wait
check_connectivity $ssh_up $rubble_node

# Step 4b: install MLNX_OFED driver and reboot
log=">> mlnx.log 2>&1"
for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/install-mlnx-ofed.sh ${log}; nohup sudo bash install-mlnx-ofed.sh ${log} &"
done
check_connectivity $ssh_down $rubble_node
check_connectivity $ssh_up   $rubble_node

# Step 4c: each node nvme-connects to its successor
log=">> nvmeof.log 2>&1"
for ip in $rubble_node
do
    ssh $ssh_arg $username@$ip "wget https://raw.githubusercontent.com/camelboat/my_rocksdb/lhy_dev/rubble/setup-nvmeof.sh ${log}; sudo bash setup-nvmeof.sh target ${log}" &
done
wait

for (( i=0; i<$shard_num; i++))
do
    j=$((($i+1)%$shard_num))
    ip=${rubble_node[$i]}
    next_ip=${rubble_node[$j]}
    ssh $ssh_arg $username@$ip "sudo bash setup-nvmeof.sh host ${next_ip} ${log}" &
done
wait

echo "Done!"
