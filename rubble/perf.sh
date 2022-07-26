#!/bin/bash

if [ $# != 2 ]; then
  echo "Usage: bash perf.sh name port"
  exit
fi

name=$1
port=$2
pid=`ps aux | grep "$name $port" | awk '{print $2}' | head -n 1`

perf record -F 99 -a -g -- sleep 120
perf script > ${name}.perf
/mnt/code/FlameGraph/stackcollapse-perf.pl ${name}.perf > ${name}.folded
/mnt/code/FlameGraph/flamegraph.pl ${name}.folded > ${name}.svg
