import sys
import numpy as np
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
    sys.exit("Usage: python3 plot-compaction-cpu.py suffix")

suffix = sys.argv[1]

workload_num = 5
config_num = 4

idx = {'load':0, 'a':1, 'b':2, 'c':3, 'd':4}

labels = ['Load', 'A', 'B', 'C', 'D']

config = ['baseline-primary', 'baseline-secondary', \
          'rubble-primary',   'rubble-secondary', \
          'rubble-offload-primary', 'rubble-offload-secondary']

data = {"baseline-compaction-primary"   : [0, 0, 0, 0, 0],
        "baseline-dbserver-primary"     : [0, 0, 0, 0, 0],
        "baseline-misc-primary"         : [0, 0, 0, 0, 0],
        "baseline-compaction-secondary" : [0, 0, 0, 0, 0],
        "baseline-dbserver-secondary"   : [0, 0, 0, 0, 0],
        "baseline-misc-secondary"       : [0, 0, 0, 0, 0],
        "rubble-offload-compaction-primary"     : [0, 0, 0, 0, 0],
        "rubble-offload-dbserver-primary"       : [0, 0, 0, 0, 0],
        "rubble-offload-misc-primary"           : [0, 0, 0, 0, 0],
        "rubble-offload-compaction-secondary"   : [0, 0, 0, 0, 0],
        "rubble-offload-dbserver-secondary"     : [0, 0, 0, 0, 0],
        "rubble-offload-misc-secondary"         : [0, 0, 0, 0, 0]}

def count_cpu(word, pid_map, workload, tok):
    ppid = word[1]
    cpu = float(word[10]) / 100
    match = False
    for role in pid_map.keys():
        if ppid in pid_map[role]:
            tok = tok + '-' + role
            data[tok][idx[workload]] += cpu
            match = True
            break

for workload in labels:
    for mode in ['baseline', 'rubble-offload']:
        workload = workload.lower()
        top_fname = 'top-' + mode + '-' + workload + '-' + suffix + '.out'
        pid_fname = 'pids-' + mode + '-' + workload + '-' + suffix + '.out'
        pid_map  = {'primary':[], 'secondary':[]}

        # 1. get pids of primary and secondary
        with open(pid_fname, 'r') as f:
            line = f.readline()
            while line:
                word = line.split()
                pid = word[1]
                if len(word) > 16 and word[16] == './db_node':
                    if int(word[20]) == 0:
                        pid_map['primary'].append(pid)
                    else:
                        pid_map['secondary'].append(pid)
                line = f.readline()

        # 2. get the CPU utilization data
        with open(top_fname, 'r') as f:
            line = f.readline()
            time = 0
            while line:
                word = line.split()
                if len(word) == 14 and word[0].isnumeric():
                    cmd = word[-1]
                    if cmd == 'COMMAND':
                        time += 1
                    elif cmd.startswith('rocksdb'):
                        count_cpu(word, pid_map, workload, mode+'-compaction')
                    elif cmd.startswith('grpcpp_sync_ser'):
                        count_cpu(word, pid_map, workload, mode+'-dbserver')
                    else:
                        count_cpu(word, pid_map, workload, mode+'-misc')
                line = f.readline()

for key in data:
    print(key, ','.join(map(str, data[key])))

# x = np.arange(workload_num)
# width = 1.0 / (1 + config_num)

# plt.figure()
# fig, ax = plt.subplots()
# rects = list()
# for idx, role in enumerate(['baseline-primary', 'baseline-secondary', 'rubble-offload-primary', 'rubble-offload-secondary']):
#     center = x + (idx - (config_num - 1) / 2.0) * width
#     tok = role + '-compaction'
#     rects.append(ax .bar(center, data[tok], width, color='red', label=role.replace('-', ' ').replcate('offload', '')))
#     tok = role + '-dbserver'
#     rects.append(ax .bar(center, data[tok], width, color='blue', bottom=data[role+'compaction'], label=role.replace('-', ' ').replcate('offload', '')))
#     tok = role + '-misc'
#     rects.append(ax .bar(center, data[tok], width, color='green', bottom=data[role+'dbserver'], label=role.replace('-', ' ').replcate('offload', '')))

# # ax.set_ylabel('CPU Consumption (%)')
# ax.set_ylabel('CPU Time (s)')
# ax.set_xticks(x, labels)
# ax.legend()

# # for r in rects:
# #     ax.bar_label(r, padding=3, rotation='vertical')

# fig.tight_layout()

# plt.savefig('top-cpu-' + suffix + '.jpg')
# plt.close()