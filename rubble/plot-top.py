import sys
import numpy as np
import matplotlib.pyplot as plt

workload_num = 5
config_num = 4

idx = {'load':0, 'a':1, 'b':2, 'c':3, 'd':4, \
       'baseline-primary':0, 'baseline-secondary':1, \
       'rubble-primary':2, 'rubble-secondary':3}

labels = ['Load', 'A', 'B', 'C', 'D']

config = ['baseline-primary', 'baseline-secondary', \
          'rubble-primary', 'rubble-secondary']

data = [[0.0, 0.0, 0.0, 0.0, 0.0],\
        [0.0, 0.0, 0.0, 0.0, 0.0],\
        [0.0, 0.0, 0.0, 0.0, 0.0],\
        [0.0, 0.0, 0.0, 0.0, 0.0]]

for workload in ['load', 'a', 'b', 'c', 'd']:
    for mode in ['baseline', 'rubble']:
        top_fname = 'top-' + mode + '-' + workload + '.out'
        pid_fname = 'pids-' + mode + '-' + workload + '.out'
        pid_map  = {'primary':[], 'secondary':[]}

        # 1. get pids of primary and secondary
        with open(pid_fname, 'r') as f:
            line = f.readline()
            while line:
                word = line.split()
                pid = word[1]
                if word[16] == './primary_node':
                    pid_map['primary'].append(pid)
                elif word[16] == './tail_node':
                    pid_map['secondary'].append(pid)
                else:
                    sys.exit('Parse Error in PID!')
                line = f.readline()

        # 2. get the CPU utilization data
        with open(top_fname, 'r') as f:
            line = f.readline()
            time = 0
            while line:
                word = line.split()
                if len(word) == 14:
                    cmd = word[-1]
                    if cmd == 'COMMAND':
                        time += 1
                    elif cmd.startswith('rocksdb'):
                        ppid = word[1]
                        cpu = float(word[9])
                        match = False
                        for role in pid_map.keys():
                            if ppid in pid_map[role]:
                                tok = mode + '-' + role
                                data[idx[tok]][idx[workload]] += cpu
                                match = True
                                break
                        if not match:
                            sys.exit('Parse Error in TOP!')
                line = f.readline()
            data[idx[mode+'-primary']][idx[workload]] /= time
            data[idx[mode+'-secondary']][idx[workload]] /= time

print(data)

x = np.arange(workload_num)
width = 1.0 / (1 + config_num)

plt.figure()
fig, ax = plt.subplots()
rects = list()
for i in range(config_num):
    center = x + (i - (config_num - 1) / 2.0) * width
    rects.append(ax.bar(center, data[i], width, label=config[i]))

ax.set_ylabel('CPU Consumption (%)')
ax.set_xticks(x, labels)
ax.legend()

for r in rects:
    ax.bar_label(r, padding=3, rotation='vertical')

fig.tight_layout()

plt.savefig('top-cpu.jpg')
plt.close()