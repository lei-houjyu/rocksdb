import sys
import numpy as np
import matplotlib.pyplot as plt

# for w in 'load' 'a' 'b' 'c' 'd'; do 
#   grep Throughput ycsb-baseline-$w-2-shard-2-replica.out \
#     | tail -n 1 \
#     | awk '{printf "%.1f, ",$(NF)}'; 
# done
data_cf1 = {'baseline (rf=2) compaction':[451854, 35658.5, 2064.2, 0, 13894.8],                                                                                                                            
            'baseline (rf=2) grpc':[147955.4, 218899, 101476.1, 72395.9, 55365.7],                         
            'baseline (rf=2) misc':[50990.600000000006, 23042.5, 120059.69999999998, 142804.1, 41939.5],
            'rubble (rf=2) compaction':[229280, 19697.9, 1323.0, 0, 8082.799999999999],                                                                                                            
            'rubble (rf=2) grpc':[172396.7, 216117, 101285.7, 73849.8, 56652.3],                   
            'rubble (rf=2) misc':[119123.29999999999, 25385.100000000006, 120991.3, 141750.2, 42464.899999999994],                                                                                 
            'rubble w/o offloading (rf=2) compaction':[228250, 0, 789.9000000000001, 0, 7913.5],                     
            'rubble w/o offloading (rf=2) grpc':[173824.1, 0, 101067.4, 73749.70000000001, 56679.5],                      
            'rubble w/o offloading (rf=2) misc':[111925.9, 800, 119742.70000000001, 141050.3, 43007.0],                   
            'baseline (rf=3) compaction':[411303, 82432.0, 4302.5, 0, 30518.6],                                                                                                                            
            'baseline (rf=3) grpc':[148639.1, 448290, 168759.2, 109342.6, 84999.3],
            'baseline (rf=3) misc':[26057.899999999994, 34478.0, 177338.3, 214257.4, 65282.09999999999],   
            'rubble (rf=3) compaction':[148763.3, 31276.0, 1682.6, 0, 11946.2],                    
            'rubble (rf=3) grpc':[184287.0, 448050, 167844.5, 112833.4, 85731.9],                  
            'rubble (rf=3) misc':[166549.7, 49474.0, 174872.90000000002, 215966.6, 76321.9],
            'rubble w/o offloading (rf=3) compaction':[0, 30653.4, 1404.5, 0, 12151.0],
            'rubble w/o offloading (rf=3) grpc':[0, 447507, 169094.0, 110048.4, 85683.3],
            'rubble w/o offloading (rf=3) misc':[1200, 48639.59999999998, 174301.5, 215951.6, 72965.7],
            'baseline (rf=4) compaction':[736847, 146049.9, 6486.0, 0, 54969.200000000004],
            'baseline (rf=4) grpc':[254209.5, 753696, 242263.69999999998, 148503.0, 104381.7],
            'baseline (rf=4) misc':[27343.5, 59854.09999999998, 275650.30000000005, 300697.0, 111849.09999999999],
            'rubble (rf=4) compaction':[211686.09999999998, 40739.4, 1832.7, 0, 15521.6],
            'rubble (rf=4) grpc':[287761.5, 754121, 242301.80000000002, 149510.7, 101457.79999999999],
            'rubble (rf=4) misc':[272952.4, 72339.59999999998, 280265.5, 299689.3, 148220.6],
            'rubble w/o offloading (rf=4) compaction':[213573.0, 39131.0, 2009.7, 0, 15216.099999999999],
            'rubble w/o offloading (rf=4) grpc':[291532.5, 754018, 242916.2, 147765.9, 102530.4],
            'rubble w/o offloading (rf=4) misc':[269294.5, 71251.0, 279474.1, 301434.1, 148653.5]}

data_cf2 = {'baseline compaction (rf=2)':[359073, 73655.2, 3382.4, 0, 27675.1],
            'baseline grpc (rf=2)':[126966.5, 441950, 253050, 196643.0, 126923.0],
            'baseline misc (rf=2)':[13560.5, 10794.799999999988, 41167.59999999998, 65357.0, 19001.899999999994],
            'rubble-offload compaction (rf=2)':[188018.40000000002, 39955.5, 2163.9, 0, 15979.400000000001],
            'rubble-offload grpc (rf=2)':[133618.0, 423324, 253884, 201695.4, 126560.8],
            'rubble-offload misc (rf=2)':[41963.59999999998, 16720.5, 42352.09999999998, 78704.6, 19459.800000000003],
            'rubble compaction (rf=2)':[0, 41411.5, 2199.0, 0, 16257.599999999999],
            'rubble grpc (rf=2)':[0, 430764, 250923, 207158, 127364.5],
            'rubble misc (rf=2)':[800, 17024.5, 42878.0, 79642, 20777.899999999994],
            'baseline compaction (rf=3)':[812882, 166803.2, 7545.900000000001, 0, 63637.600000000006],
            'baseline grpc (rf=3)':[282786.2, 895996, 394994, 276309.4, 169426.4],
            'baseline misc (rf=3)':[17531.79999999999, 21600.800000000047, 184660.09999999998, 226890.59999999998, 66936.0],
            'rubble-offload compaction (rf=3)':[318614, 62327.399999999994, 2767.7, 0, 23413.399999999998],
            'rubble-offload grpc (rf=3)':[294857.6, 898240, 393608, 267733.1, 163687.0],
            'rubble-offload misc (rf=3)':[186528.40000000002, 29432.599999999977, 190824.30000000005, 235466.90000000002, 110499.59999999998],
            'rubble compaction (rf=3)':[320573, 60443.1, 2806.6000000000004, 0, 23137.5],
            'rubble grpc (rf=3)':[292509.1, 896094, 394640, 268878.1, 164549.9],
            'rubble misc (rf=3)':[179317.90000000002, 34662.90000000002, 189353.40000000002, 234321.90000000002, 109512.6],
            'baseline compaction (rf=4)':[1483160, 295622.7, 13636.5, 0, 110972.6],
            'baseline grpc (rf=4)':[504703, 1529544, 535705, 340344.19999999995, 221557.5],
            'baseline misc (rf=4)':[26137, 38833.30000000005, 493458.5, 553655.8, 199869.90000000002],
            'rubble-offload compaction (rf=4)':[448158, 81952.70000000001, 3806.2, 0, 30949.8],
            'rubble-offload grpc (rf=4)':[533468, 1544917, 535919, 331821.0, 218417.10000000003],
            'rubble-offload misc (rf=4)':[437574, 68730.30000000005, 503074.80000000005, 562179.0, 276233.1],
            'rubble compaction (rf=4)':[449066, 82848.2, 0, 0, 30632.100000000002],
            'rubble grpc (rf=4)':[528272, 1541270, 0, 338473.9, 218404.9],
            'rubble misc (rf=4)':[435462, 83481.80000000005, 1600, 555126.1, 275363.0]}

color_dict={'baseline (rf=2)' :              'darkgreen',
            'rubble (rf=2)'   :              'seagreen',
            'rubble w/o offloading (rf=2)' : 'lightgreen',

            'baseline (rf=3)' :              'navy',
            'rubble (rf=3)'   :              'royalblue',
            'rubble w/o offloading (rf=3)' : 'deepskyblue',

            'baseline (rf=4)' :              'crimson',
            'rubble (rf=4)'   :              'fuchsia',
            'rubble w/o offloading (rf=4)' : 'pink'}

def plot(data, figure_name):
    workloads = ['Load', 'A', 'B', 'C', 'D']
    breakdown = 3

    x = np.arange(len(workloads))
    config_num = len(data.keys()) / breakdown
    width = 1.0 / (1 + config_num)

    plt.figure()
    fig, ax = plt.subplots()
    rects = list()

    r_list = [2, 3, 4]
    m_list = ['baseline', 'rubble', 'rubble w/o offloading']
    c_list = ['compaction', 'grpc', 'misc']
    for r in r_list:
        for m in m_list:
            for c in c_list:
                i = r_list.index(r) * len(m_list) + m_list.index(m)
                key= m + ' (rf={}) '.format(r) + c
                center = x + (i - (config_num - 1) / 2.0) * width
                bot = None
                if c == 'grpc':
                    bot = data[m + ' (rf={}) '.format(r) + 'compaction']
                elif c == 'misc':
                    bot = data[m + ' (rf={}) '.format(r) + 'grpc']

                print(key, data[key], bot)
                rects.append(ax.bar(center, data[key], width, label=m + ' (rf={}) '.format(r), bottom=bot))


    ax.set_ylabel('CPU Time (s)')
    ax.set_xticks(x, workloads)
    ax.legend()

    fig.tight_layout()

    plt.savefig(figure_name)
    plt.close()

plot(data_cf1, 'cpu-cf1.pdf')
# plot(data_cf2, 'ycsb-thru-cf2.pdf')