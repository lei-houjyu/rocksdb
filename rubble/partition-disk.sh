#!/bin/bash

sfdisk /dev/nvme0n1 < partition.dump

yes | mkfs.ext4 /dev/nvme0n1p1
yes | mkfs.ext4 /dev/nvme0n1p2

mkdir /mnt/data /mnt/sst

mount /dev/nvme0n1p1 /mnt/data
mount /dev/nvme0n1p2 /mnt/sst
