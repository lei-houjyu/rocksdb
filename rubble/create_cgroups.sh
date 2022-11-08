#!/bin/bash

if [ $# -lt 1 ];
then
    echo "Usage: bash create_cgroups.sh cpu_num"
    exit
fi

cpu=`bash first_n_cpu.sh $1`

cgcreate -g memory:/rubble-mem
cgcreate -g cpuset:/rubble-cpu

cgset -r memory.limit_in_bytes=256G rubble-mem
cgset -r cpuset.cpus=$cpu rubble-cpu
cgset -r cpuset.mems=0 rubble-cpu
