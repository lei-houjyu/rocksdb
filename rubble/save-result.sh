#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage: bash save-result.sh suffix"
    exit
fi

suffix=$1
cp primary-1.out primary-1-${suffix}.out
cp primary-2.out primary-2-${suffix}.out
cp tail-1.out tail-1-${suffix}.out
cp tail-2.out tail-2-${suffix}.out
cp dstat.csv dstat-${suffix}.csv
cp iostat.out iostat-${suffix}.out
cp pids.out pids-${suffix}.out
cp top.out top-${suffix}.out

cp /mnt/data/db/1/primary/db/LOG LOG-primary-1-${suffix}
cp /mnt/data/db/2/primary/db/LOG LOG-primary-2-${suffix}
cp /mnt/data/db/1/tail/db/LOG LOG-tail-1-${suffix}
cp /mnt/data/db/2/tail/db/LOG LOG-tail-2-${suffix}
