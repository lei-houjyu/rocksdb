#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage: bash dstat.sh cpu_num"
    exit
fi

cpu=`bash first_n_cpu.sh $1`

mv dstat.csv old-dstat.csv

dstat -cdt -C $cpu --output dstat.csv
