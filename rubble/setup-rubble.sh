#!/bin/bash

# DATA_PATH holds code repo, RocksDB files
DATA_PATH="/mnt/data"
# SST_PATH holds the SST pool used by Rubble
SST_PATH="/mnt/sst"

setup_nvmeof_offloading() {
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

install_dependencies() {
    apt install -y build-essential autoconf libtool pkg-config libgflags-dev \
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
}

setup_grpc() {
    GPRC_VERSION=1.34.0
    NUM_JOBS=`nproc`

    MY_INSTALL_DIR=/root
    mkdir -p $MY_INSTALL_DIR

    export PATH="$PATH:$MY_INSTALL_DIR/bin"

    cd ${DATA_PATH}

    if [ ! -d './grpc' ]; then
        git clone --recurse-submodules -b v${GPRC_VERSION} https://github.com/grpc/grpc
    fi

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

    for (( i=1; i<=2; i++ ));
    do
        for r in primary tail;
        do
            for f in db sst_dir;
            do
                mkdir -p ${DATA_PATH}/db/${i}/${r}/${f} 
            done
        done
        mkdir -p ${SST_PATH}/${i}
        bash create-sst-pool.sh 16777216 4 5000 ${SST_PATH}/${i}
    done

    umount ${SST_PATH}
    mount -o ro,noload /dev/nvme0n1p2 $SST_PATH
}

setup_nvmeof_offloading
install_dependencies
partition_disk
setup_grpc
setup_rocksdb

echo "Done!"
