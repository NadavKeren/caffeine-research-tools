import numpy as np 

from math import fsum

import tarfile

from os.path import *
from os import path, listdir, makedirs

from typing import Generator, List, Dict

from utils import Colors, Timer

FloatGenerator = Generator[float, None, None]

INPUT_DIR = './processed'
OUTPUT_DIR = './out_latencies'

class DistGenerators():
    @staticmethod
    def create_normal(mean: float, std_div: float) -> FloatGenerator:
        random_gen = np.random.default_rng()
        def gen_next():
            while True:
                val = random_gen.normal(mean, std_div)
                yield max(val, mean - 3 * std_div, 5), mean
        
        return gen_next()
    
    @staticmethod
    def create_uniform(low: float, high: float) -> FloatGenerator:
        mean = (low + high * 1.0) / 2
        random_gen = np.random.default_rng()
        def gen_next():
            while True:
                yield random_gen.uniform(low, high), mean

        return gen_next()
    
    @staticmethod
    def create_two_peaks(low_val: float, high_val: float, prob: float) -> FloatGenerator:
        values = [low_val, high_val]
        mean = low_val * prob + high_val * (1 - prob)
        probs = [prob, 1 - prob]
        
        def gen_next():
            while True:
                yield np.random.choice(values, p=probs), mean
                
        return gen_next()

    @staticmethod
    def create_single_val(val: float) -> FloatGenerator:
        def gen_next():
            while True:
                yield val, val
                
        return gen_next()
    

def compressTrace(output_file_name: str, should_remove=False):
    with Timer(msg='compression') as t:
        with tarfile.open(f'{output_file_name}.xz', mode="w:xz") as tar:
            tar.add(f'{output_file_name}.trace')
            
    if should_remove:
        run(['rm', f'{output_file_name}.trace'])

      
class KeyInfo():
    def __init__(self, dist_generator: FloatGenerator):
        self.dist_gen = dist_generator
        self.occurences = 1
        
def printHistograms(latency_values: np.array, keysTimeDistDict: Dict[str, KeyInfo], run_num: int):
    latencies_histogram = np.histogram(all_values, bins=[0, 10, 100, 1000, 10000])
    print(f'Latencies of run num {run_num} is:\n{str(latencies_histogram)}')
    
    occurences_aggregate = [key_info.occurences for key_info in keysTimeDistDict.values()]
    occurences_histogram = np.histogram(occurences_aggregate, bins=[0, 1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100, 1000, 10000])
    print(f'Key occurences histogram is:\n{str(occurences_histogram)}')
        
        
def addDelayAndWriteToFile(fnames: List[str], time_generators: List[FloatGenerator], 
                           cluster_dists: List[float], run_num: int, hit_penalty=1):
    latency_values = np.array([])
    
    keysTimeDistDict : Dict[str, KeyInfo] = dict()
    
    output_file_name = f'{OUTPUT_DIR}/latency_penalties_{run_num}'
    
    with open(f'{output_file_name}.trace', 'w') as outputFile:
        for fname in fnames:
            with open(f'{INPUT_DIR}/{fname}') as inputFile:
                lines = inputFile.readlines()
                num_of_lines = len(lines)
                
                current_file_latencies = np.zeros(num_of_lines)                
                
                for idx in range(num_of_lines):
                    line = lines[idx]
                    key = line.strip('\n ')
                    
                    current_key_info : KeyInfo = keysTimeDistDict.get(key)
                    
                    if not current_key_info is None:
                        dist_gen = current_key_info.dist_gen
                        current_key_info.occurences += 1
                    else:
                        chosen_cluster = np.random.choice(range(len(time_generators)), p=cluster_dists)
                        dist_gen = time_generators[chosen_cluster]
                        keysTimeDistDict[key] = KeyInfo(dist_gen)
                    
                    delay, mean = next(dist_gen)
                    
                    current_file_latencies[idx] = delay
                    
                    outputFile.write(f'{key} {hit_penalty} {delay} {mean}\n')
                
                print(f'{Colors.orange}Added latencies to {Colors.cyan}{len(lines):,} '
                    + f'{Colors.orange} lines with {Colors.cyan}{len(keysTimeDistDict):,}'
                    + f'{Colors.orange} unique entries so far{Colors.reset}')

                latency_values = np.concatenate((latency_values, current_file_latencies))
    
    printHistograms(latency_values, keysTimeDistDict, run_num)
    
    # compressTrace(output_file_name)
        
def verifyDists(cluster_dist: List[float], num_of_generators : int):
    dist_sum: float = fsum(cluster_dist)
    if dist_sum != 1.0 or num_of_generators != len(cluster_dist):
        raise ValueError(f'Bad configuration {dist_sum}, num of generators: {num_of_generators} ' +
                         f'and num of clusters {len(cluster_dist)}')
        
        
def main():
    # cluster_dists = [[0.25, 0.5, 0.25], [0.4, 0.2, 0.2, 0.2], [0.4, 0.35, 0.25]] # precentage of requestes with this index range of times
    # time_generators = [[UniformDist(50, 150),
    #                     UniformDist(125, 250),
    #                     UniformDist(200, 300)],
    #                    [UniformDist(50, 200),
    #                     NormalDist(64, 12),
    #                     NormalDist(96, 12),
    #                     NormalDist(128, 18)],
    #                    [NormalDist(80, 64),
    #                     NormalDist(100, 64),
    #                     NormalDist(120, 64)]]
    
    cluster_dists = [[0.7, 0.3]] # , [0.5, 0.5], [0.7, 0.3]]
    time_generators = [[DistGenerators.create_two_peaks(5, 5000, 0.9), DistGenerators.create_single_val(50)]]
                    #    [DistGenerators.create_two_peaks(5, 5000, 0.2), DistGenerators.create_single_val(4500)],
                    #    [DistGenerators.create_two_peaks(5, 5000, 0.2), DistGenerators.create_single_val(4500)]]
    
    for i in range(len(cluster_dists)):
        verifyDists(cluster_dists[i], len(time_generators[i]))
        
    input_files_paths = [f for f in listdir(INPUT_DIR)]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    for i in range(len(time_generators)):
        with Timer(f'penalties configuartion No. {i + 1}'):
            addDelayAndWriteToFile(input_files_paths, time_generators[i], cluster_dists[i], i+1)
            
if __name__ == '__main__':
    main()
