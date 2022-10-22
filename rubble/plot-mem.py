# top, press f, chose to display PPID, press q, press W (upper case)
from ast import parse
import sys
import numpy as np
import matplotlib.pyplot as plt

suffix = sys.argv[1]

data = {'baseline-primary'  : [], 'rubble-primary'   : [], \
        'baseline-secondary': [], 'rubble-secondary' : []}
    
def parse_mem(s):
    if s.isnumeric():
        return float(s) / 1024 / 1024
    elif s.endswith('m'):
        return float(s[:-1]) / 1024
    elif s.endswith('g'):
        return float(s[:-1])
    else:
        sys.exit('Parse Error!')

for mode in ['baseline', 'rubble']:
    top_fname = 'top-' + mode + '-' + suffix + '.out'
    pid_fname = 'pids-' + mode + '-' + suffix + '.out'
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
        while line:
            word = line.split()
            if len(word) == 14:
                cmd = word[-1]
                if cmd == 'COMMAND':
                    for key in data:
                        if mode in key:
                            data[key].append(0)
                elif cmd == 'primary_node' and data[mode+'-primary'][-1] == 0:
                    data[mode+'-primary'][-1] = parse_mem(word[6]) + parse_mem(word[7])
                elif cmd == 'tail_node' and data[mode+'-secondary'][-1] == 0:
                    data[mode+'-secondary'][-1] = parse_mem(word[6]) + parse_mem(word[7])

            line = f.readline()

plt.figure()
for key in data.keys():
    time = len(data[key])
    x = np.arange(time)
    plt.plot(x, data[key], label=key)

plt.xlabel('Second')
plt.ylabel('Physical + Swap Memory (GB)')
plt.legend()
fig_name = suffix+'-mem.jpg'
plt.savefig(fig_name)
print('result is saved as', fig_name)
plt.close()
