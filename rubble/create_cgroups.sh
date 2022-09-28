#!/bin/bash

cgcreate -g memory:/rubble-mem
cgcreate -g cpuset:/rubble-cpu

cgset -r memory.limit_in_bytes=8G rubble-mem
cgset -r cpuset.cpus=0,2,4,6 rubble-cpu
cgset -r cpuset.mems=0 rubble-cpu
