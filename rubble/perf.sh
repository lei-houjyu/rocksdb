#!/bin/bash

if [ $# != 1 ]; then
  echo "Usage: bash perf.sh pid_fname"
  exit
fi

fname=$1
pid=`cat ${fname}`

perf record -p $pid -F 499 -C 0-3 -g
perf script > ${fname}.perf
