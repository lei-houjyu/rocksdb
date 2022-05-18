#!/bin/bash

if [ $# != 3 ]; then
    echo "Usage: bash change-ini.sh file_name attribute_key attribute_value"
    exit
fi

file=$1
key=$2
val=$3

sed -i "s/${key}=.*/${key}=${val}/g" $file
