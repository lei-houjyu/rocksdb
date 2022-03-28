#!/bin/bash

procs=`lsof | grep -E '/mnt/sdb|/mnt/remote' | awk '{print $2}'`
sudo kill -9 $procs;

umount /dev/nvme0n1p4
umount /dev/nvme1n1p4