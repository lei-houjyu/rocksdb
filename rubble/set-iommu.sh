#!/bin/bash

set -x

if [ $# -lt 1 ]
then
    echo "Usage: bash set-iommu.sh status(on/off)"
    exit
fi

status=$1
parameter="intel_iommu="$status
grub_file="/etc/default/grub"

if dmesg | grep iommu | grep -q "amd"
then
    parameter="amd_iommu="$status
fi

echo "Adding booting parameter ${parameter} to ${grub_file}"

old=`grep GRUB_CMDLINE_LINUX_DEFAULT ${grub_file}`

new=${old::-1}${parameter}\"

sed -i "s/${old}/${new}/g" $grub_file

update-grub

echo "Rebooting"

reboot
