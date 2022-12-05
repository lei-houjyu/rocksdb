import sys
import matplotlib.pyplot as plt

file_name = sys.argv[1]
agg_num = int(sys.argv[2])
figure_name = file_name[:-4]
data = {'user':[0], 'nice':[0], 'system':[0], 'iowait':[0], 'steal':[0], 'idle':[0]}
ignore_key = ['code', 'swap']

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
                print(nums[0], should_ignore(nums[0]))
                if not should_ignore(nums[0]):
                    read_key  = nums[0] + '-read'
                    write_key = nums[0] + '-write'
                    if read_key not in data:
                        data[read_key] = [0]
                    if write_key not in data:
                        data[write_key] = [0]                    
                    data[read_key][-1] += float(nums[2]) / 1024
                    data[write_key][-1] += float(nums[3]) / 1024
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
for key in data:
    if 'read' in key or 'write' in key:
        plt.plot(data['time'], data[key], label=key)
        print(key, 'sum', sum(data[key]), 'avg', sum(data[key]) / len(data['user']))
plt.xlabel('Second')
plt.ylabel('Throughput (MB/s)')
plt.ylim([0, 1000])
plt.legend()
# plt.show()
plt.savefig(figure_name + '-disk.pdf')
plt.close()
