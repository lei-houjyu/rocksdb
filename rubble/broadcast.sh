#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: bash broadcast.sh number_of_nodes command"
    exit
fi

node_num=$1
cmd=$2
path=`pwd`
green="\033[0;32m"
blue="\033[0;34m"
nc="\033[0m"

host=`hostname`
host=${host::5}
if [ $host != "node1" ]; then
    echo "Error: must run this script on node1"
    exit
fi

for (( i=0; i<$node_num; i++ )); do
    ip="10.10.1."$(( $i + 2 ))
    echo -e ${green}${ip}${nc} ${blue}${cmd}${nc}
    ssh ${USER}@${ip} "cd $path; $cmd"
done