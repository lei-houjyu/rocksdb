#!/bin/bash

if [ $# != 2 ]; then
    echo "Usage: bash show-mem.sh pid-file top-file"
    exit
fi

pids=`awk '{print $2}' $1`

for i in $pids;
do
  grep $i $1 | awk '{print "command:", $(NF-3)}'
  awk -v pid=$i '{if ($2==pid) {
                    v=$6;
                    p=$7;
                    s=$8;
                  }; 
                  if ($14=="COMMAND" && v != 0 && p != 0) {
                    t++;
                    if (v>vmax) vmax=v;
                    if (p>pmax) pmax=p;
                    if (s>smax) smax=s;
                    vsum+=v;
                    psum+=p;
                    ssum+=s;
                    v=0;
                    p=0;
                    s=0;
                  }
                 }END{
                  print "VIRT:", vsum/t/1024/1024, vmax/1024/1024, "PHY", psum/t/1024/1024, pmax/1024/1024, "SWAP", ssum/t/1024/1024, smax/1024/1024;
                 }' $2
done
