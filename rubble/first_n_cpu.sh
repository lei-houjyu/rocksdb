#!/bin/bash
# Get the first n CPUs in numa node 0 in the format
# 0,1,2,...,n

if [ $# -lt 1 ];
then
    echo "Usage: bash first_n_cpu.sh cpu_num"
    exit
fi

n=$1

cpu=`numactl -H | grep "node 0 cpus:"`

array=($cpu)

for (( i=0; i<$n; i++ ))
do
    res=${res}${array[$(($i + 3))]}","
done

echo ${res::-1}