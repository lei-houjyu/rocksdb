#!/usr/bin/python3

import sys

if len(sys.argv) != 4:
    print('usage: python3 network-breakdown.py suffix shards runtime')
    exit(0)
    

suffix = sys.argv[1]
shards = int(sys.argv[2])
# original runtime is in ms
runtime = int(sys.argv[3]) / 1000
# workload = sys.argv[4]

nethogs_fname = 'nethogs-' + suffix + '.out'

grpc_usage = 0.0
# times = 0
occured = False
with open(nethogs_fname, 'r') as f:
    for line in reversed(f.readlines()):
        if line.startswith('./db_node'):
            toks = line.strip().split()
            # only rx traffic
            grpc_usage += float(toks[1]) + float(toks[2])
            occured = True
        if line.startswith('Refresh') and occured:
            # times += 1
            break


sst_load_phase_cnt = {}
with open('LOAD_PHASE_SST_MARK_%s.out' % suffix, 'r') as f:
    for line in f.readlines():
        toks = line.split(',')
        s = int(toks[0].split()[1])
        cnt = int(toks[1])
        sst_load_phase_cnt[s] = cnt


sst_cnt = 0
for s in range(shards):
    log_fname = 'LOG-shard-' + str(s) + '-' + suffix
    with open(log_fname, 'r') as f:
        for line in reversed(f.readlines()):
            if 'Shipped SST file' in line:
                sst_cnt += int(line.split(':')[-1].strip()) - sst_load_phase_cnt[s]
                break


print("shard: %d, runtime: %fs" % (shards, runtime))
print('grpc network usage: %f MB/s' % (grpc_usage / runtime))
print('NVMe-oF network usage (approximation): %f MB/s' % (sst_cnt * 17 / runtime))
