#!/bin/bash

if [ $# != 2 ]; then
  echo "Usage: bash perf.sh name port"
  exit
fi

name=$1
port=$2
pid=`ps aux | grep "$name $port" | awk '{print $2}' | head -n 1`

perf record -p $pid -F 499 -C 0-3 -g
perf script > ${name}.perf
