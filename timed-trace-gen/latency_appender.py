import numpy as np 

from math import fsum, inf

import tarfile
import datetime

from os.path import *
from os import path, listdir, makedirs

from typing import Generator, List, Dict

from utils import Colors, Timer
from json import dump

# FloatGenerator = Generator[float, None, None]

INPUT_DIR = './processed'
OUTPUT_DIR = './out_latencies'

RANDOM_BATCH_SIZE = 10000000

class NormalDist():
    def __init__(self, mean: float, std_div: float):
        self._std_div = std_div
        self._random_gen = np.random.default_rng()
        
        self.mean = mean

        self.refill_values()
    
    def refill_values(self):
        self.index = 0
        self.gen_values = self._random_gen.normal(self.mean, self._std_div, size=RANDOM_BATCH_SIZE)
        
        for i in range(RANDOM_BATCH_SIZE):
            val = self.gen_values[i]
            self.gen_values[i] = max(val, self.mean - 3 * self._std_div, 5)
    
    def __str__(self):
        return f'Normal with mean {self.mean} and sigma {self._std_div}'


class UniformDist():
    def __init__(self, low: float, high: float):
        self._low = low
        self._high = high
        self._random_gen = np.random.default_rng()
        
        self.mean = (low + high * 1.0) / 2

        self.refill_values()
        
    def refill_values(self):
        self.index = 0
        self.gen_values = self._random_gen.uniform(self._low, self._high, size=RANDOM_BATCH_SIZE)
        
    def __str__(self):
        return f'Uniform between {self._low} and {self._high}'
    

class TwoPeakDist():
    def __init__(self, low_val: float, high_val: float, prob: float):
        self._values = [low_val, high_val]
        self._probs = [prob, 1 - prob]
        
        self.mean = low_val * prob + high_val * (1 - prob)
        
        self.refill_values()
    
    def refill_values(self):
        self.index = 0
        self.gen_values = np.random.choice(self._values, p=self._probs, size=RANDOM_BATCH_SIZE)
        
    def __str__(self):
        return f'Two Peaks with values {self._values} at probabilty {self._probs}'
       
     
class SingleValueDist():
    def __init__(self, val: float):
        self.mean = val
        self.gen_values = [val] * RANDOM_BATCH_SIZE
        
        self.refill_values()
        
        
    def refill_values(self):
        self.index = 0
    
    def __str__(self):
        return f'Single Value of {self.mean}'


def compressTrace(output_file_name: str, should_remove=False):
    with Timer(msg='compression') as t:
        with tarfile.open(f'{output_file_name}.xz', mode="w:xz") as tar:
            tar.add(f'{output_file_name}.trace')
            
    if should_remove:
        run(['rm', f'{output_file_name}.trace'])

      
class KeyInfo():
    def __init__(self, dist_generator):
        self.dist_gen = dist_generator
        self.occurences = 1


def writeMetaData(fname: str, time_generators: List, cluster_dists: List[float], latency_values: np.array, keysTimeDistDict: Dict[str, KeyInfo], run_num: int):
    cluster_data = [{'probability': cluster_dists[i], 'dist info': str(time_generators[i])} for i in range(len(time_generators))]
    
    latencies_histogram, latencies_bins = np.histogram(latency_values, bins=[0, 10, 100, 1000, 10000, inf])
    latencies_data = [{'histogram': latencies_histogram.tolist(), 'bins': latencies_bins.tolist()}]
    
    occurences_aggregate = [key_info.occurences for key_info in keysTimeDistDict.values()]
    occurences_histogram, occurences_bins = np.histogram(occurences_aggregate, bins=[1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100, 1000, 10000, inf])
    occurences_data = [{'histogram': occurences_histogram.tolist(), 'bins': occurences_bins.tolist()}]
    
    data = {'clusters data': cluster_data, 'latencies data': latencies_data, 'occurences data': occurences_data}
    
    with open(f'{fname}_conf{run_num}.json', 'w') as jsonFile:
        dump(data, jsonFile, indent=4)

        
def addDelayAndWriteToFile(fnames: List[str], time_generators: List, 
                           cluster_dists: List[float], run_num: int, hit_penalty=1):
    latency_values = np.array([])
    
    keysTimeDistDict : Dict[str, KeyInfo] = dict()
    
    current_time = datetime.datetime.now()
    
    output_file_name = f'{OUTPUT_DIR}/latency_p{run_num}_{current_time.hour}{current_time.minute}{current_time.second}{current_time.day}{current_time.month}'
    
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
                    
                    delay, mean = dist_gen.gen_values[dist_gen.index], dist_gen.mean
                    dist_gen.index += 1
                    
                    if dist_gen.index >= RANDOM_BATCH_SIZE:
                        dist_gen.refill_values()
                    
                    current_file_latencies[idx] = delay
                    
                    outputFile.write(f'{key} {hit_penalty} {delay} {mean}\n')
                
                print(f'{Colors.orange}Added latencies to {Colors.cyan}{len(lines):,} '
                    + f'{Colors.orange} lines with {Colors.cyan}{len(keysTimeDistDict):,}'
                    + f'{Colors.orange} unique entries so far{Colors.reset}')

                latency_values = np.concatenate((latency_values, current_file_latencies))
    
    writeMetaData(output_file_name, time_generators, cluster_dists, latency_values, keysTimeDistDict, run_num)
    
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
    
    # cluster_dists = [[0.7, 0.3] , [0.5, 0.5], [0.7, 0.3], [0.5, 0.5]]
    # time_generators = [[DistGenerators.create_two_peaks(5, 5000, 0.9), DistGenerators.create_single_val(50)],
    #                    [DistGenerators.create_two_peaks(5, 5000, 0.2), DistGenerators.create_single_val(4500)],
    #                    [DistGenerators.create_two_peaks(5, 5000, 0.2), DistGenerators.create_single_val(4500)],
    #                    [DistGenerators.create_two_peaks(5, 1000000, 0.9999), DistGenerators.create_single_val(50)]]
    
    # cluster_dists = [[0.5, 0.5], [0.5, 0.5], [0.5, 0.5], [0.5, 0.5]]
    # time_generators = [[TwoPeakDist(5, 5000, 0.9), SingleValueDist(50)],
    #                    [TwoPeakDist(5, 500, 0.9), SingleValueDist(50)],
    #                    [TwoPeakDist(5, 100, 0.9), SingleValueDist(10)],
    #                    [TwoPeakDist(5, 50, 0.9), SingleValueDist(10)]]
    
    cluster_dists = [[0.5, 0.5]]
    time_generators = [[TwoPeakDist(5, 1000000, 0.9999), SingleValueDist(50)]]
    
    for i in range(len(cluster_dists)):
        verifyDists(cluster_dists[i], len(time_generators[i]))
        
    input_files_paths = [f for f in listdir(INPUT_DIR)]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    for i in range(len(time_generators)):
        with Timer(f'penalties configuartion No. {i + 1}'):
            addDelayAndWriteToFile(input_files_paths, time_generators[i], cluster_dists[i], i+1)
            
if __name__ == '__main__':
    main()
