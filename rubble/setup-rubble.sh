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
    sfdisk /dev/nvme0n1 < partition.dump

    yes | mkfs.ext4 /dev/nvme0n1p1
    yes | mkfs.ext4 /dev/nvme0n1p2

    mkdir $DATA_PATH $SST_PATH

    mount /dev/nvme0n1p1 $DATA_PATH
    mount /dev/nvme0n1p2 $SST_PATH

    lsblk
}

setup_grpc() {
    GPRC_VERSION=1.34.0
    NUM_JOBS=`nproc`

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
    cd ${DATA_PATH}

    git clone --branch lhy_dev https://github.com/camelboat/my_rocksdb.git

    cd my_rocksdb

    bash build.sh

    cd rubble

    for (( i=1; i<=$1; i++ ));
    do
        for r in primary tail;
        do
            for f in db sst_dir;
            do
                mkdir -p ${DATA_PATH}/db/${i}/${r}/${f} 
            done
        done
        mkdir -p ${SST_PATH}/${i}
        bash create-sst-pool.sh 16777216 4 5000 ${SST_PATH}/${i} &
    done
    wait

    umount ${SST_PATH}
    mount -o ro,noload /dev/nvme0n1p2 $SST_PATH
}

install_dependencies
partition_disk
setup_grpc
setup_rocksdb $1

echo "Done!"
