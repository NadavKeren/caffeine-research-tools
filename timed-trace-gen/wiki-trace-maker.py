import numpy as np 
import mmh3
from datetime import timedelta
import time
from os.path import *
from os import path, listdir, makedirs
from typing import Generator, List, Dict

FloatGenerator = Generator[float, None, None]

IGNORED_URL_TAGS = ["?search=", "&search=", "User+talk", "User_talk","User:", "Talk:", "&diff=", "&action=rollback", "Special:Watchlist"]
IGNORED_PREFIXES = ["wiki/Special:Search", "w/query.php","wiki/Talk:", "wiki/Special:AutoLogin", "Special:UserLogin", "w/api.php", "error:"]
OUTPUT_FILE_PREFIX = './out_wiki/processed_'

class Timer():
    def __init__(self):
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    
    def __exit__(self, *exec_info):
        self.end_time = time.time()
        diff = self.end_time - self.start_time
        print(f'Elapsed time: {timedelta(seconds=diff)}')

      
def create_normal_dist_generator(mean: float, std_div: float):
    random_gen = np.random.default_rng()
    def gen_next():
        while True:
            val = random_gen.normal(mean, std_div)
            yield max(val, mean - 3 * std_div, 5), mean
    
    return gen_next()

def create_uniform_dist_generator(low: float, high: float):
    mean = (low + high * 1.0) / 2
    random_gen = np.random.default_rng()
    def gen_next():
        while True:
            yield random_gen.uniform(low, high), mean

    return gen_next()

def parseWikiLine(entry: str):   
    url = entry.split(' ')[2]
    
    for prefix in IGNORED_PREFIXES:
        if url.startswith(prefix):
            return None
        for tag in IGNORED_URL_TAGS:
            if tag in url:
                return None
    
    return url      

    
def processFile(fname: str):
    with open(f'./input/{fname}', encoding='utf-8',errors='replace') as wiki:
        with open(f'{OUTPUT_FILE_PREFIX}{fname}','w') as f:
            line = wiki.readline()
            count = 1
            while line:
                url = parseWikiLine(line)
                if url != None:
                    key = str(int.from_bytes(mmh3.hash_bytes(url)[-8:],'big'))
                    f.write('%s\n'%key)
                line = wiki.readline()
                count += 1


def createWIKITrace(fname: str, time_generators: List[FloatGenerator], cluster_dists: List[float], run_num: int, hit_penalty=1, prec99=False):
    with open(f'{OUTPUT_FILE_PREFIX}{fname}') as wiki:
        keys = wiki.readlines()
        m = len(keys)
        dic = dict()
        with open(f'out_wiki/{fname}_penalties{run_num}{"_with99prec" if prec99 else ""}.trace', 'w') as f:
            for i in range(m):
                key = keys[i].strip('\n ')
                
                if dic.get(key):
                    dist_gen = dic.get(key)
                else:
                    chosen_cluster = np.random.choice(range(len(time_generators)), p=cluster_dists)
                    dist_gen = time_generators[chosen_cluster]
                    dic[key] = dist_gen
                
                delay, mean = next(dist_gen)
                
                # if prec99 and np.random.rand() >= 0.99:
                #     delay *= prec99_factor
                
                f.write(f'{key} {hit_penalty} {delay} {mean}\n')
        
        print(f'{m} lines with {len(dic)} unique entries time-generated')


def verifyDists(cluster_dist: List[float], time_generators: List[FloatGenerator]):
    dist_sum = sum(cluster_dist)
    if dist_sum != 1 or len(time_generators) != len(cluster_dist):
        raise ValueError("Bad configuration")
        

def main():
    cluster_dists = [[1], [1]] # precentage of requestes with this index range of times
    time_generators = [[create_normal_dist_generator(1024, 256)], [create_uniform_dist_generator(50, 150)]]
    
    verifyDists(cluster_dists[0], time_generators[0])
    
    input_files_paths = [f for f in listdir('./input') if isfile(join('./input', f)) and f.startswith('wiki')] # can be downloaded from http://www.wikibench.eu/wiki and other wiki traces too
    
    makedirs('./out_wiki', exist_ok=True)
    print(f'processing the files: {input_files_paths}')
    for wikifile in input_files_paths:
        if not path.exists(f'{OUTPUT_FILE_PREFIX}{wikifile}'):
            print(f'start filtering {wikifile}')
            with Timer():
                processFile(wikifile)
            
            print(f'done filtering: {wikifile}')
        
        
        for i in range(len(time_generators)):
            with Timer():
                createWIKITrace(wikifile, time_generators[i], cluster_dists[i], i+1)
                # createWIKITrace(f'processed_{wikifile}', time_generators[i], cluster_dists[i], i+1, prec99=True)
                print('done creating penalties trace number %d for %s'%(i+1,wikifile))
     
if __name__ == '__main__':
    main()
