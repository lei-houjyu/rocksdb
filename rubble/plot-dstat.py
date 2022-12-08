import sys
import matplotlib.pyplot as plt

file_name = sys.argv[1]
agg_num = int(sys.argv[2])
cpu_num = int(sys.argv[3])
figure_name = file_name[:-4]
data = {'user':[], 'system':[], 'idle':[], 'wait':[], 'steal':[], 'read':[], 'write':[]}

agg_cnt = 0
with open(file_name, 'r') as f:
    line = f.readline()
    
    while line:
        if not line.startswith('\"'):
            if agg_cnt % agg_num == 0:
                for key in data:
                    if len(data[key]) > 0:
                        data[key][-1] /= agg_num
                    data[key].append(0)

            agg_cnt += 1
            nums = line.split(',')

            for i in range(cpu_num):
                data['user'][-1]   += float(nums[0 + i * 5])
                data['system'][-1] += float(nums[1 + i * 5])
                data['idle'][-1]   += float(nums[2 + i * 5])
                data['wait'][-1]   += float(nums[3 + i * 5])
                data['steal'][-1]  += float(nums[4 + i * 5])

            data['read'][-1]  += float(nums[-3]) / 1024 / 1024
            data['write'][-1] += float(nums[-2]) / 1024 / 1024

        line = f.readline()

    if agg_cnt % agg_num != 0:
        for key in data:
            if len(data[key]) > 0:
                data[key][-1] /= agg_num

data['time'] = [i * agg_num for i in range(len(data['user']))]

plt.figure()
for key in ['user', 'system', 'idle', 'wait', 'steal']:
    plt.plot(data['time'], data[key], label=key)
    print(key, sum(data[key]) / len(data[key]))

plt.xlabel('Second')
plt.ylabel('Utilization (%)')
plt.ylim([0, cpu_num * 100])
plt.legend()
plt.savefig(figure_name + '-cpu.pdf')
plt.close()

plt.figure()
for key in ['read', 'write']:
    plt.plot(data['time'], data[key], label=key)
    print(key, sum(data[key]))
plt.xlabel('Second')
plt.ylabel('Throughput (MB/s)')
plt.ylim([0, 1000])
plt.legend()
plt.savefig(figure_name + '-disk.pdf')
plt.close()
