#!/bin/bash

if [ $# != 1 ]; then
  echo "Usage: bash backup.sh shard_num"
fi

cp -r /mnt/data/db/ /mnt/backup

for i in $(seq $1); do
done