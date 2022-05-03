import numpy as np 

from math import fsum, inf

import argparse
import datetime
import tarfile
import tqdm

from os.path import *
from os import path, listdir, makedirs

from typing import Generator, List, Dict

from functools import reduce

from utils import Colors, Timer
from json import dump

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


class MultiplePeaksDist():
    def __init__(self, values : List[float], probs : List[float]):
        self._values = values
        self._probs = probs
        
        self.mean = reduce(lambda acc, curr: acc + curr[0] * curr[1], zip(self._values, self._probs), 0)
        self.refill_values()
    
    def refill_values(self):
        self.index = 0
        self.gen_values = np.random.choice(self._values, p=self._probs, size=RANDOM_BATCH_SIZE)
        
    def __str__(self):
        return f'{len(self._values)} Peaks with values {self._values} and probabilty {self._probs}'

     
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


def writeMetaData(fname: str, time_generators: List, cluster_dists: List[float], latency_values: np.array, keysTimeDistDict: Dict[str, KeyInfo]):
    cluster_data = [{'probability': cluster_dists[i], 'dist info': str(time_generators[i])} for i in range(len(time_generators))]
    
    latencies_histogram, latencies_bins = np.histogram(latency_values, bins=[0, 10, 100, 1000, 10000, inf])
    latencies_data = [{'histogram': latencies_histogram.tolist(), 'bins': latencies_bins.tolist()}]
    
    occurences_aggregate = [key_info.occurences for key_info in keysTimeDistDict.values()]
    occurences_histogram, occurences_bins = np.histogram(occurences_aggregate, bins=[1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 100, 1000, 10000, inf])
    occurences_data = [{'histogram': occurences_histogram.tolist(), 'bins': occurences_bins.tolist()}]
    
    data = {'clusters data': cluster_data, 'latencies data': latencies_data, 'occurences data': occurences_data}
    
    with open(f'{fname}_conf.json', 'w') as jsonFile:
        dump(data, jsonFile, indent=4)

        
def addDelayAndWriteToFile(fnames: List[str], time_generators: List, cluster_dists: List[float], 
                           verbose: bool, compress: bool, hit_penalty=1):
    latency_values = np.array([])
    
    keysTimeDistDict : Dict[str, KeyInfo] = dict()
    
    current_time = datetime.datetime.now()
    
    if len(fnames) > 1:
        output_file_name = f'{OUTPUT_DIR}/latency_{current_time.strftime("%H%M%S_%d%m%Y")}'
    else:
        output_file_name = f'{OUTPUT_DIR}/{fnames[0]}_{current_time.strftime("%H%M%S_%d%m%Y")}'
    
    with open(f'{output_file_name}.trace', 'w') as outputFile:
        for fname, _ in zip(fnames, tqdm.tqdm(range(len(fnames)), colour='yellow', leave=False)):
            with open(f'{INPUT_DIR}/{fname}') as inputFile:
                lines = inputFile.readlines()
                num_of_lines = len(lines)
                
                current_file_latencies = np.zeros(num_of_lines)                
                
                for idx in tqdm.trange(num_of_lines, colour='cyan', leave=False):
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
                    
                    """
                    Here, using the fields instead of functions in order to reduce the call time.
                    Moreover, the usage of batches lowers the computation time by 90%!
                    """
                    delay, mean = dist_gen.gen_values[dist_gen.index], dist_gen.mean
                    dist_gen.index += 1
                    
                    if dist_gen.index >= RANDOM_BATCH_SIZE:
                        dist_gen.refill_values()
                    
                    current_file_latencies[idx] = delay
                    
                    outputFile.write(f'{key} {hit_penalty} {delay} {mean}\n')
                
                if (verbose):
                    print(f'{Colors.orange}Added latencies to {Colors.cyan}{len(lines):,} '
                        + f'{Colors.orange} lines with {Colors.cyan}{len(keysTimeDistDict):,}'
                        + f'{Colors.orange} unique entries so far{Colors.reset}')

                latency_values = np.concatenate((latency_values, current_file_latencies))
    
    writeMetaData(output_file_name, time_generators, cluster_dists, latency_values, keysTimeDistDict)
    
    if (compress):
        compressTrace(output_file_name)
        
def verifyDists(cluster_dist: List[float], num_of_generators : int):
    dist_sum: float = fsum(cluster_dist)
    if dist_sum != 1.0 or num_of_generators != len(cluster_dist):
        raise ValueError(f'Bad configuration {dist_sum}, num of generators: {num_of_generators} ' +
                         f'and num of clusters {len(cluster_dist)}')
        
        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-c', '--compress', help="Compress the newly created traces files", action='store_true')
    parser.add_argument('-v', '--verbose', help='Prints the time elapsed and number of unique entries for each file, in addition to the progress bar', action='store_true')
    
    args = parser.parse_args()
    
    cluster_dists = [[1]]
    
    time_generators = [[MultiplePeaksDist([75, 300, 1000], [0.8, 0.15, 0.05])]]
    
    for i in range(len(cluster_dists)):
        verifyDists(cluster_dists[i], len(time_generators[i]))
        
    input_files_paths = [f for f in listdir(INPUT_DIR)]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    with Timer():
        for i in tqdm.trange(len(time_generators), colour='magenta', leave=False):
            addDelayAndWriteToFile(input_files_paths, time_generators[i], cluster_dists[i], verbose=args.verbose, compress=args.compress)
            
if __name__ == '__main__':
    main()
