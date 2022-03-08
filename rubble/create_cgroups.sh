#!/bin/bash

cgcreate -g memory:/rubble-mem
cgcreate -g cpuset:/rubble-cpu

cgset -r memory.limit_in_bytes=1G rubble-mem
cgset -r cpuset.cpus=0-7 rubble-cpu
cgset -r cpuset.mems=0 rubble-cpu
