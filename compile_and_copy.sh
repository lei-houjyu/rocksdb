#!/bin/bash

make clean

make -j32 rocksdbjava

mkdir -p /root/.m2/repository/org/rocksdb/rocksdbjni/6.14.0 && cp ./java/target/rocksdbjni-6.14.0-linux64.jar /root/.m2/repository/org/rocksdb/rocksdbjni/6.14.0/rocksdbjni-6.14.0.jar;
mkdir -p /root/.m2/repository/org/rocksdb/rocksdbjni/6.13.3 && cp ./java/target/rocksdbjni-6.14.0-linux64.jar /root/.m2/repository/org/rocksdb/rocksdbjni/6.13.3/rocksdbjni-6.13.3.jar;
mkdir -p /mnt/sdb/YCSB/rocksdb/target/dependency && cp -p ./java/target/rocksdbjni-6.14.0-linux64.jar /mnt/sdb/YCSB/rocksdb/target/dependency/rocksdbjni-6.13.3.jar;
