#!/bin/bash

if [ $# != 1 ]; then
  echo "Usage: bash rsync.sh rf"
  exit
fi

rf=$1

for (( i=2; i<=$rf; i++ )); do
    rsync -avz --delete --include '*/' --include '*.cc' --include '*.h' --include '*.proto' --include '*.py' --include '*.sh' --exclude '*' /mnt/data/rocksdb/ root@node-$i:/mnt/data/rocksdb/
done