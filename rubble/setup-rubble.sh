#!/bin/bash

set -x

nvme_dev='/dev/nvme0n1'
DATA_PATH="/mnt/data"
SST_PATH="/mnt/sst"

install_dependencies() {
    apt update
    apt install -y build-essential autoconf libtool pkg-config libgflags-dev htop \
                   dstat sysstat cgroup-tools cmake python3-pip nvme-cli numactl \
                   linux-tools-generic linux-tools-`uname -r`
    pip3 install matplotlib
}

# After this function, the disk will look like:
# nvme0n1
# ├─nvme0n1p1
# ├─nvme0n1p2
# └─nvme0n1p3
# /dev/nvme0n1p1 will be mounted to /mnt/data, and /dev/nvme0n1p{2..N} will 
# be mounted to /mnt/sst/shard-x, which holds secondary-x's SST files in Rubble
partition_disk() {
    lsblk

    local shard_num=$1
    local rf=$2

    local sst_size=100
    local data_size=$(( 50 + shard_num * 16 ))
    local secondary_num=$(( shard_num / rf * (rf - 1) ))
    
    wipefs $nvme_dev

    local unit_str="G
    "

    local partition_str="n
    p


    +"

    local sync_str="w
    "

    local cmd_str="$partition_str"${data_size}"$unit_str"

    for (( i=0; i<$secondary_num; i++ ))
    do
        cmd_str="${cmd_str}${partition_str}"${sst_size}"${unit_str}"
    done
    cmd_str="${cmd_str}${sync_str}"

    echo "$cmd_str" | fdisk $nvme_dev
    
    while [[ -z $(lsblk | grep nvme0n1p2) ]]; do
        sleep 1
    done

    for dev in `ls ${nvme_dev}p*`
    do
        yes | mkfs.ext4 $dev
    done

    mkdir $DATA_PATH $SST_PATH

    mount ${nvme_dev}p1 $DATA_PATH

    lsblk
}

setup_grpc() {
    local GPRC_VERSION=1.34.0
    local NUM_JOBS=`nproc`

    MY_INSTALL_DIR=/root
    mkdir -p $MY_INSTALL_DIR

    export PATH="$PATH:$MY_INSTALL_DIR/bin"

    cd ${DATA_PATH}

    git clone --recurse-submodules -b v${GPRC_VERSION} https://github.com/grpc/grpc

    cd grpc
    mkdir -p cmake/build
    pushd cmake/build
    cmake -DgRPC_INSTALL=ON \
        -DgRPC_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
        ../..
    make -j${NUM_JOBS}
    make install
    popd

    echo "grpc build success, building hellp world example "

    cd ${DATA_PATH}/grpc/examples/cpp/helloworld
    mkdir -p cmake/build
    pushd cmake/build
    cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../..
    make -j

    echo "export PATH=/root:$PATH" >> /root/.bashrc
    source /root/.bashrc

    echo "hello world example build success"
}

setup_rocksdb() {
    source helper.sh

    lsblk

    local shard_num=$1
    local rf=$2
    
    cd ${DATA_PATH}

    git clone --branch lhy_dev https://github.com/camelboat/my_rocksdb.git

    cd my_rocksdb

    bash build.sh

    cd rubble

    local pid=2
    local nid=$( get_nid )
    for (( sid=0; sid<${shard_num}; sid++ ));
    do
        for f in db sst_dir;
        do
            mkdir -p ${DATA_PATH}/db/shard-${sid}/${f} 
        done
        local ret=$( is_head $nid $sid $rf )
        if [ "$ret" == "false" ]
        then
            local mount_point=${SST_PATH}/shard-${sid}
            mkdir -p $mount_point
            mount ${nvme_dev}p${pid} $mount_point
            pid=$(( pid + 1 ))
            touch ${mount_point}/node-${nid}-shard-${sid}.txt
            bash create-sst-pool.sh 16777216 4 5000 $mount_point > /dev/null 2>&1 &
        fi
    done
    wait

    for dev in `ls ${nvme_dev}p*`
    do
        umount $dir
    done

    lsblk
}

install_dependencies
partition_disk $1 $2
setup_grpc
setup_rocksdb $1 $2

echo "Done!"
