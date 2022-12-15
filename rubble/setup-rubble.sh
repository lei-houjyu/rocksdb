#!/bin/bash

set -x

DATA_PATH="/mnt/data"
SST_PATH="/mnt/sst"

install_dependencies() {
    apt update
    apt install -y build-essential autoconf libtool pkg-config libgflags-dev htop \
                   dstat sysstat cgroup-tools cmake python3-pip nvme-cli numactl \
                   linux-tools-generic linux-tools-`uname -r`
    pip3 install matplotlib
}

partition_disk() {
    # get partition.dump by 'sfdisk -d /dev/nvme0n1'
    sfdisk /dev/nvme0n1 < partition.dump
    while [[ -z $(lsblk | grep nvme0n1p2) ]]; do
        sleep 1
    done

    for dev in `ls /dev/nvme0n1p*`
    do
        yes | mkfs.ext4 $dev
    done

    mkdir $DATA_PATH $SST_PATH

    mount /dev/nvme0n1p1 $DATA_PATH
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

is_head()
{
    local sid=$1
    local rf=$2
    local ip=$(hostname -I | awk '{print $2}' | tail -c 2)
    local idx=$(($ip - 2))
    if [ $(($sid % $rf)) -eq $idx ]; then
        echo "true"
    else
        echo "false"
    fi
}

setup_rocksdb() {
    local shard_num=$1
    local rf=$2
    
    cd ${DATA_PATH}

    git clone --branch lhy_dev https://github.com/camelboat/my_rocksdb.git

    cd my_rocksdb

    bash build.sh

    cd rubble

    local pnum=2
    for (( i=0; i<${shard_num}; i++ ));
    do
        for f in db sst_dir;
        do
            mkdir -p ${DATA_PATH}/db/shard-${i}/${f} 
        done
        local val=$( is_head $i $rf )
        if [ "$val" == "false" ]
        then
            mkdir -p ${SST_PATH}/shard-${i}
            mount /dev/nvme0n1p${pnum} ${SST_PATH}/shard-${i}
            pnum=$(( pnum + 1 ))
            bash create-sst-pool.sh 16777216 4 5000 ${SST_PATH}/shard-${i} &
        fi
    done
    wait

    for dev in `ls /dev/nvme0n1p* | grep -v nvme0n1p1`
    do
        dir=`lsblk -o MOUNTPOINT -nr $dev`
        umount $dir
        mount -o ro,noload $dev $dir
    done
}

install_dependencies
partition_disk
setup_grpc
setup_rocksdb $1 $2

echo "Done!"
