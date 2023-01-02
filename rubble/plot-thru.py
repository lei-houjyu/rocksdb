import sys
from datetime import datetime
import matplotlib.pyplot as plt

file_name = sys.argv[1]
agg_num = int(sys.argv[2])
data = {'read':[], 'write':[], 'queued_op':[], 'queued_edit':[]}

with open(file_name, 'r') as f:
    line = f.readline()
    
    cnt = 0
    while line:
        if line.startswith('[READ]') and line.endswith('2022\n'):
            nums = line.split()
            if nums[1] != '0' or nums[6] != '0':
                if cnt == 0:
                    for key in data:
                        if len(data[key]) > 0:
                            data[key][-1] /= agg_num
                        data[key].append(0)
                data['read'][-1] += int(nums[3])
                data['write'][-1] += int(nums[8])
                data['queued_op'][-1] += int(nums[13])
                data['queued_edit'][-1] += int(nums[15])
                cnt = (cnt + 1) % agg_num

        line = f.readline()

data['time'] = [i for i in range(len(data['read']))]

plt.figure()
for key in ['read', 'write']:
    plt.plot(data['time'], data[key], label=key)
    print(max(data[key]))

plt.xticks(rotation = 45)
plt.xlabel('Time')
plt.ylabel('OP/s')
plt.ylim([0,100000])
plt.legend()
plt.savefig(file_name[:-4] + '-thru.jpg')
print(file_name[:-4] + '-thru.jpg')
plt.close()

plt.figure()
fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

ax1.plot(data['time'], data['queued_op'], label='queued_op', color='r')
ax2.plot(data['time'], data['queued_edit'], label='queued_edit', color='b')

plt.xticks(rotation = 45)
ax1.set_xlabel('Time')
ax1.set_ylabel('Queued OP', color='r')
ax2.set_ylabel('Queued Edit', color='b')
plt.savefig(file_name[:-4] + '-queue.jpg')
print(file_name[:-4] + '-queue.jpg')
plt.close()