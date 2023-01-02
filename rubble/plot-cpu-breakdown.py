import sys
import numpy as np
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
    sys.exit("Usage: python3 plot-compaction-cpu.py suffix")

suffix = sys.argv[1]
agg_num = int(sys.argv[2])

workload_num = 5
config_num = 4

idx = {'load':0, 'a':1, 'b':2, 'c':3, 'd':4}

labels = ['Load', 'A', 'B', 'C', 'D']

config = ['baseline-primary', 'baseline-secondary', \
          'rubble-primary',   'rubble-secondary', \
          'rubble-primary', 'rubble-secondary']

data = {"baseline-compaction-primary"   : [0, 0, 0, 0, 0],
        "baseline-dbserver-primary"     : [0, 0, 0, 0, 0],
        "baseline-misc-primary"         : [0, 0, 0, 0, 0],
        "baseline-compaction-secondary" : [0, 0, 0, 0, 0],
        "baseline-dbserver-secondary"   : [0, 0, 0, 0, 0],
        "baseline-misc-secondary"       : [0, 0, 0, 0, 0],
        "rubble-compaction-primary"     : [0, 0, 0, 0, 0],
        "rubble-dbserver-primary"       : [0, 0, 0, 0, 0],
        "rubble-misc-primary"           : [0, 0, 0, 0, 0],
        "rubble-compaction-secondary"   : [0, 0, 0, 0, 0],
        "rubble-dbserver-secondary"     : [0, 0, 0, 0, 0],
        "rubble-misc-secondary"         : [0, 0, 0, 0, 0]}

time_series_data = {}

def agg(arr):
    res = []
    print('len(arr)', len(arr))
    for i in range(len(arr)):
        if i % agg_num == 0:
            if len(res) > 0:
                res[-1] /= agg_num
            res.append(arr[i])
        else:
            res[-1] += arr[i]
    res[-1] /= len(arr) % agg_num
    return res

def count_cpu(word, pid_map, workload, mode, job, time):
    ppid = word[1]
    category = mode + '-' + job
    cpu = float(word[10]) / 100
    for role in pid_map.keys():
        if ppid in pid_map[role]:
            # 1. for aggregation data
            tok = category + '-' + role
            data[tok][idx[workload]] += cpu

            # 2. for time series data
            if job in ['compaction', 'dbserver']:
                tok = job + '-' + mode + '-' + ppid
                if tok not in time_series_data.keys():
                    time_series_data[tok] = [0]
                else:
                    if len(time_series_data[tok]) < time:
                        time_series_data[tok].append(0)
                    time_series_data[tok][-1] += cpu

            break

for workload in labels:
    for mode in ['baseline', 'rubble']:
        time = 0
        time_series_data.clear()
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
            while line:
                word = line.split()
                if len(word) == 14 and word[0].isnumeric():
                    cmd = word[-1]
                    if cmd.startswith('rocksdb'):
                        count_cpu(word, pid_map, workload, mode, 'compaction', time)
                    elif cmd.startswith('grpcpp_sync_ser'):
                        count_cpu(word, pid_map, workload, mode, 'dbserver', time)
                    else:
                        count_cpu(word, pid_map, workload, mode, 'misc', time)
                elif len(word) == 14 and word[-1] == 'COMMAND':
                    time += 1
                line = f.readline()

        # 3. plot time series data
        plt.figure()
        time_axis = [i for i in range(int(time / agg_num) + int(time % agg_num > 0))]
        print('time_axis', len(time_axis))
        for key in time_series_data.keys():
            print(key, len(time_series_data[key]))
            plt.plot(time_axis, agg(time_series_data[key]), label=key)
        figure_name = 'real-time-cpu-' + mode + '-' + workload + '-' + suffix + '.jpg'
        print('real time cpu stats are plot in ' + figure_name)
        plt.xlabel('Time')
        plt.ylabel('CPU Utilization')
        plt.ylim([0, 1])
        plt.legend()
        plt.savefig(figure_name)
        plt.close()
    break

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