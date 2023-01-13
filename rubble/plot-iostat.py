import sys
import matplotlib.pyplot as plt

file_name = sys.argv[1]
agg_num = int(sys.argv[2])
figure_name = file_name[:-4]
data = {'user':[0], 'nice':[0], 'system':[0], 'iowait':[0], 'steal':[0], 'idle':[0]}
ignore_key = ['code', 'swap']
tot_read = 0
tot_write = 0

def should_ignore(key):
    for i in ignore_key:
        if i in key:
            return True
    return False

agg_cnt = 0
with open(file_name, 'r') as f:
    line = f.readline()
    while line:
        if line[0] == 'a':
            agg_cnt += 1
            # avg-cpu:  %user   %nice %system %iowait  %steal   %idle
            line = f.readline()
            nums = line.split()
            data['user'][-1] += float(nums[0])
            data['nice'][-1] += float(nums[1])
            data['system'][-1] += float(nums[2])
            data['iowait'][-1] += float(nums[3])
            data['steal'][-1] += float(nums[4])
            data['idle'][-1] += float(nums[5])
        elif line[0] == 'D':
            # Device             tps    kB_read/s    kB_wrtn/s    kB_read    kB_wrtn
            line = f.readline()
            while len(line) > 0 and line[0] == 'n':
                nums = line.split()
                if not should_ignore(nums[0]):
                    read_key  = nums[0] + '-read'
                    write_key = nums[0] + '-write'
                    if read_key not in data:
                        data[read_key] = [0]
                    if write_key not in data:
                        data[write_key] = [0]                    
                    data[read_key][-1] += float(nums[2]) / 1024
                    data[write_key][-1] += float(nums[3]) / 1024
                    tot_read += float(nums[2])
                    tot_write += float(nums[3])
                line = f.readline()
            if agg_cnt % agg_num == 0:
                for key in data:
                    if len(data[key]) > 0:
                        data[key][-1] /= agg_num
                    data[key].append(0)
        line = f.readline()
    if agg_cnt % agg_num != 0:
        for key in data:
            if len(data[key]) > 0:
                data[key][-1] /= agg_num

duration = len(data['user'])
data['time'] = [i * agg_num for i in range(duration)]

print('agg_cnt', agg_cnt)
for key in data:
    print(key, len(data[key]))

plt.figure()
for key in ['user', 'nice', 'system', 'iowait', 'steal', 'idle']:
    plt.plot(data['time'], data[key], label=key)
    print(key, 'sum', sum(data[key]), 'avg', sum(data[key]) / len(data['user']))
plt.xlabel('Second')
plt.ylabel('Utilization (%)')
plt.legend()
# plt.show()
plt.savefig(figure_name + '-cpu.jpg')
plt.close()

plt.figure()
fs=15
# for key in data:
#     if 'read' in key or 'write' in key:
#         plt.plot(data['time'][5:31], data[key][5:31], label=key)
#         print(key, 'sum', sum(data[key]), 'avg', sum(data[key]) / len(data['user']))

# rubble
plt.plot(data['time'][5:31], data['nvme0c0n1-read'][5:31], '.-', label='Local Read')
plt.plot(data['time'][5:31], data['nvme0c0n1-write'][5:31], '^-', label='Local Write')
plt.plot(data['time'][5:31], data['nvme1c1n1-write'][5:31], 'd-', label='Remote Write (Secondary 1)')
plt.plot(data['time'][5:31], data['nvme2c2n1-write'][5:31], 'v-', label='Remote Write (Secondary 2)')

# baseline
# plt.plot(data['time'][8:49], data['nvme0c0n1-read'][8:49], '.-', label='Local Read')
# plt.plot(data['time'][8:49], data['nvme0c0n1-write'][8:49], '^-', label='Local Write')

plt.xlabel('Second', fontsize=fs)
plt.ylabel('Throughput (MB/s)', fontsize=fs)
plt.xticks(fontsize=fs)
plt.yticks(fontsize=fs)
plt.ylim([200, 700])
plt.legend()
# plt.show()
plt.savefig(figure_name + '-disk.pdf')
plt.close()

print('tot_read:', tot_read, 'total_write:', tot_write)