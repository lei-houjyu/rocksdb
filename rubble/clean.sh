#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Usage: bash clean.sh shard_num"
fi

rm -rf log/* core*
for (( i=0; i<$1; i++ )); do
  rm -rf /mnt/data/db/shard-$i/db/* \
          /mnt/data/db/shard-$i/sst_dir/* \
          /mnt/data/db/shard-$i.out
done