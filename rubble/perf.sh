#!/bin/bash

if [ $# != 3 ]; then
  echo "Usage: bash perf.sh name cpuset time"
  exit
fi

name=$1
#port=$2
#pid=`ps aux | grep "$name $port" | awk '{print $2}' | head -n 1`
cpu=$2
time=$3

cp perf.data ${name}.perf.data
perf record -F 99 -C ${cpu} -g -- sleep ${time}
perf script > ${name}.perf
/mnt/data/FlameGraph/stackcollapse-perf.pl ${name}.perf > ${name}.folded
/mnt/data/FlameGraph/flamegraph.pl ${name}.folded > ${name}.svg
