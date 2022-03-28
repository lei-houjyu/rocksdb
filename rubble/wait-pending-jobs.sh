#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage bash wait-pending-jobs.sh name"
    exit
fi

name=$1

a=-1
b=`grep JOB /mnt/sdb/archive_dbs/${name}/db/LOG | wc -l`

while [ $a != $b ]; do
    a=$b
    b=`grep JOB /mnt/sdb/archive_dbs/${name}/db/LOG | wc -l`
    sleep 10
done

sudo killall ${name}_node 