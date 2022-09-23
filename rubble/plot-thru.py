import sys
from datetime import datetime
import matplotlib.pyplot as plt

file_name = sys.argv[1]
data = {'read':[], 'write':[], 'queued_op':[], 'queued_edit':[], 'time':[]}

with open(file_name, 'r') as f:
    line = f.readline()
    
    while line:
        if line.startswith('[READ]') and line.endswith('2022\n'):
            nums = line.split()
            if nums[1] != '0' or nums[6] != '0':
                data['read'].append(int(nums[3]))
                data['write'].append(int(nums[8]))
                data['queued_op'].append(int(nums[13]))
                data['queued_edit'].append(int(nums[15]))

                time_obj = datetime.strptime(nums[-2], '%H:%M:%S')
                data['time'].append(time_obj)

        line = f.readline()

plt.figure()
for key in ['read', 'write']:
    plt.plot(data['time'], data[key], label=key)
    print(max(data[key]))

plt.xticks(rotation = 45)
plt.xlabel('Time')
plt.ylabel('OP/s')
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