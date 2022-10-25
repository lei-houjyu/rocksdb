#!/bin/bash

cd /mnt/data

git clone --branch lhy_dev https://github.com/camelboat/my_rocksdb.git

cd my_rocksdb

bash build.sh

for (( i=1; i<=2; i++ ));
do
  for r in primary tail;
  do
    for f in db sst_dir;
      do
        mkdir -p /mnt/data/db/${i}/${r}/${f} 
      done
  done
  mkdir -p /mnt/sst/${i}
  bash create-sst-pool.sh 16777216 4 5000 /mnt/sst/${i}
done
