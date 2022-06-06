#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage bash wait-pending-jobs.sh log_path"
    exit
fi

log=$1

a=-1
b=`grep JOB $log | wc -l`

while [ $a != $b ]; do
    a=$b
    sleep 15
    b=`grep JOB $log | wc -l`
done
