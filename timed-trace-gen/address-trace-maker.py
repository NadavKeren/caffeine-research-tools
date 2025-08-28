import sys
import random
from functools import reduce
import math
from collections import Counter

read_times = {
    'HDD1':{
        'name':'WD VelociRaptor 1TB',
        'min':1.23,
        'avg':1.64,
        'max':1.9,
        'rl':3.0,
        'st':6.9
    },
    'HDD2':{
        'name':'Seagate Barracuda 1TB',
        'min':0.6,
        'avg':0.81,
        'max':1,
        'rl':4.1,
        'st':12.7
    },
    'HDD3':{
        'name':'Hitachi Deskstar 7K1000 1TB',
        'min':0.4,
        'avg':0.71,
        'max':0.8,
        'rl':4.1,
        'st':13
    },
    'SSD1':{
        'name':'WD Black SN750 NVMe PCIe M.2 500GB (2019)',
        'min':36.8,
        'avg':46.4,
        'max':52.6,
        'st':35
    },
    'SSD2':{
        'name':'WD Blue 500GB (2016)',
        'min':18.5,
        'avg':28.7,
        'max':38.9,
        'st':100
    }
}

def calc_thresholds(flag=False):
    trace = 'input/' + sys.argv[1]
    portions = [int(p) for p in sys.argv[2].split('#')]
    s = sum(portions)
    fracs = [p/s for p in portions]
    fracs = [x+(sum(fracs[:i]) if i > 0 else 0) for i,x in zip(range(len(fracs)),fracs)]
    print(fracs)
    addresses = set()
    with open(trace) as f:
        line = f.readline()
        while line:
            if 'format=gradle' in sys.argv:
                address = [int(line.split()[0],16)]
            elif 'format=address' in sys.argv:
                address = [int(line.split()[1],16)]
            elif 'format=arc' in sys.argv:
                address = range(int(line.split()[0]),int(line.split()[0])+int(line.split()[1]))
            elif 'format=oltp' in sys.argv:
                address = [int(line.split(',')[1])]
            elif 'format=lirs' in sys.argv:
                address = [int(line)]
            else:
                address = [int(line.split()[-1],16)]
            for a in address:
                addresses.add(a)
            line = f.readline()
    addresses = list(addresses)
    addresses.sort()
    ths = [addresses[int(len(addresses)*f) if f<1.0 else -1] for f in fracs]
    count = Counter()
    for a in addresses:
        for t in ths:
            if a <= t:
                count[str(t)] += 1
                break
    if flag:
        print('#'.join([str(t) for t in ths]))
        print(len(addresses))
        print(count)
    else:
        return ths


if __name__ == "__main__":
    input_file = 'input/'+sys.argv[1]
    ths = calc_thresholds()
    drives = sys.argv[3].split('#')
    block_sizes = [4098]
    size_names = ['4kb']
    hps = ['1.1']
    if '-s' in sys.argv:
        seed = sys.argv[sys.argv.index('-s') + 1]
    else:
        seed = 27021990
    random.seed(seed)
    with open(input_file) as f:
        for ext_name,block_size,hp in zip(size_names,block_sizes,hps):
            hp = float(hp)
            f.seek(0)
            out_file = 'out_ths/%s'%ext_name+sys.argv[1]
            with open(out_file,'w') as out:
                line = f.readline()
                while line:
                    if 'format=gradle' in sys.argv:
                        addresses = [int(line.split()[0],16)]
                    elif 'format=address' in sys.argv:
                        addresses = [int(line.split()[1],16)]
                    elif 'format=arc' in sys.argv:
                        addresses = range(int(line.split()[0]),int(line.split()[0])+int(line.split()[1]))
                    elif 'format=oltp' in sys.argv:
                        addresses = [int(line.split(',')[1])]
                    elif 'format=lirs' in sys.argv:
                        addresses = [int(line)]
                    else:
                        addresses = [int(line.split()[-1],16)]
                    if not addresses:
                        print('Error: missing format argument')
                        exit(1)
                    line = line.rstrip()
                    for address in addresses:
                        mpi = 0
                        while mpi < len(ths) and address > ths[mpi]:
                            mpi+=1
                        drive = drives[mpi]
                        min_read = read_times[drive]['min']
                        max_read = read_times[drive]['max']
                        avg_read = read_times[drive]['avg']
                        read_rate = random.triangular(min_read,max_read,avg_read)
                        mp = block_size/read_rate #gives miss penalty in microseconds
                        if 'HDD' in drive:
                            mp += read_times[drive]['rl']*1000 + read_times[drive]['st']*1000
                        else:
                            mp += read_times[drive]['st']
                        flip = random.random()
                        hit_time = hp*flip if flip > 0.95 else hp*(1.0+flip) if flip > 0.1 else hp*(2.0+flip) 
                        out.write(str(address)+' %.2f %.2f'%(hit_time,mp)+'\n')
                    line = f.readline()
    #TODO: add support for commpression 
    
