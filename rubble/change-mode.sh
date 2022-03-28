#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage: bash change-mode.sh mode"
    exit
fi

mode=$1
if [ $mode == 'rubble' ]; then
    sed -i 's/is_rubble=false/is_rubble=true/g' rubble_16gb_config.ini
    sed -i 's/is_rubble=false/is_rubble=true/g' rubble_16gb_config_tail.ini
fi

if [ $mode == 'baseline' ]; then
    sed -i 's/is_rubble=true/is_rubble=false/g' rubble_16gb_config.ini
    sed -i 's/is_rubble=true/is_rubble=false/g' rubble_16gb_config_tail.ini
fi