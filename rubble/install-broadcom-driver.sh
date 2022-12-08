#!/bin/bash

set -x

if [ $# -lt 1 ]
then
    echo "Usage: bash install-broadcom-driver.sh phase(1/2)"
    exit
fi

phase=$1
ip=`hostname -I | awk '{print $2}'`

if [ $phase -eq 1 ]; then
    wget https://docs.broadcom.com/docs-and-downloads/ethernet-network-adapters/NXE/BRCM_223.1.96.0/bcm_223.1.96.0.tar.gz
    tar zxvf bcm_223.1.96.0.tar.gz
    cd bcm_223.1.96.0/Linux/Linux_Installer/
    bash install.sh -i eno12409np1 -a $ip -n 255.255.255.0
    reboot
else
    cd bcm_223.1.96.0/Linux/Linux_Installer/
    bash install.sh -i eno12409np1 -a $ip -n 255.255.255.0
    sysctl -p
fi