#!/bin/bash
if [ $# != 2 ]; then
    echo "Usage: bash show-mem.sh pid-file top-file"
    exit
fi

pids=`awk '{print $2}' $1`

for i in $pids;
do
  grep $i $1 | awk '{print "command:", $(NF-3)}'
  awk -v pid=$i '{if ($2==pid) {v+=$6; p+=$7; s+=$8;}; if ($14=="COMMAND") {t++}}END{print "VIRT:", v/t/1024/1024, "PHY", p/t/1024/1024, "SWAP", s/t/1024/1024}' $2
done