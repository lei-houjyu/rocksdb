#!/bin/bash

if [ $# != 3 ]; then
    echo "Usage: bash change-mode.sh mode(baseline/rubble) rf base_background_jobs"
    exit
fi

mode=$1
rf=$2
n=$3

if [ $mode == 'rubble' ]; then
    sed -i 's/is_rubble=false/is_rubble=true/g' rubble_16gb_config.ini
    sed -i 's/is_rubble=false/is_rubble=true/g' rubble_16gb_config_tail.ini
    sed -ir "s/max_background_jobs=[0-9]*/max_background_jobs=$(( n * rf ))/g" rubble_16gb_config.ini
fi

if [ $mode == 'baseline' ]; then
    sed -i 's/is_rubble=true/is_rubble=false/g' rubble_16gb_config.ini
    sed -i 's/is_rubble=true/is_rubble=false/g' rubble_16gb_config_tail.ini
    sed -ir "s/max_background_jobs=[0-9]*/max_background_jobs=$n/g" rubble_16gb_config.ini
    sed -ir "s/max_background_jobs=[0-9]*/max_background_jobs=$n/g" rubble_16gb_config_tail.ini
fi