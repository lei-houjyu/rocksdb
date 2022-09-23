import sys
from datetime import datetime
import matplotlib.pyplot as plt

file_name = sys.argv[1]
data = {'imm':[], 'level-0':[], 'level-1':[], 'level-2':[], 'level-3':[], 'level-4':[], 'time':[]}

with open(file_name, 'r') as f:
    line = f.readline()
    
    while line:
        if line.endswith('cfd->current\n'):
            #2022/09/15-12:01:31.619421
            time_str = line.split()[0]
            time_obj = datetime.strptime(time_str, '%Y/%m/%d-%H:%M:%S.%f')
            data['time'].append(time_obj)

            # skip 4 lines
            for i in range(4):
                line = f.readline()
            
            data['imm'].append(int(line))

            # skip 1 line
            line = f.readline()

            for i in range(5):
                line = f.readline()
                nums = line.split()
                data['level-'+str(i)].append(int(nums[4]))

        line = f.readline()

plt.figure()
for key in data.keys():
    if key != 'time':
        plt.plot(data['time'], data[key], label=key)

plt.xticks(rotation = 45)
plt.xlabel('Time')
plt.ylabel('Count')
plt.yscale('log')
plt.legend()
plt.savefig(file_name + '-lsm.jpg')
plt.close()