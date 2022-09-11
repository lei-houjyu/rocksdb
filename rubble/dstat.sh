#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage: bash dstat.sh cpus"
    exit
fi

mv dstat.csv old-dstat.csv

dstat -cdt -C $1 --output dstat.csv
