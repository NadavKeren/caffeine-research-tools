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
    __slots__ = '_std_div', '_random_gen', 'mean', 'index', 'gen_values'
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
    __slots__ = '_low', '_high', '_random_gen', 'mean', 'index', 'gen_values'
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


class MultiplePeaksDist():
    __slots__ = '_values', '_probs', 'mean', 'index', 'gen_values'
    def __init__(self, values : List[float], probs : List[float]):
        if not len(probs) == len(values):
            raise ValueError(f'length mismatch for probs: {probs} and values {values} {len(probs)} != {len(values)}')
        
        if not fsum(probs) == 1:
            raise ValueError(f'Invalid Probabilities - the sum is: {fsum(probs)}')
        
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
    __slots__ = 'mean', 'index', 'gen_values'
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
                           verbose: bool, compress: bool, hit_penalty=1, set_name: str = None):
    latency_values = np.array([])
    
    keysTimeDistDict : Dict[str, KeyInfo] = dict()
    
    current_time = datetime.datetime.now()
    
    if set_name is not None:
        output_file_name = f'{OUTPUT_DIR}/{set_name}'
    else:
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
    
    # cluster_dists = [[1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1], [1]]
    
    # probs = [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.05, 0.025, 0.025, 0.025, 0.025, 0.01, 0.01, 0.01, 0.02]
    # time_generators = [[MultiplePeaksDist([71.35, 73.91, 76.98, 78.56, 80.12, 93.59, 85.89, 91.22, 98.43, 152.87, 211.48, 317.36, 378.06, 397.36, 411.26, 454.20, 640.22], probs)],
    #                    [MultiplePeaksDist([61.83, 63.41, 67.49, 70.09, 72.22, 76.47, 84.01, 165.85, 228.51, 306.47, 359.12, 433.26, 613.03, 698.66, 822.75, 1023.51, 1482.82], probs)],
    #                    [MultiplePeaksDist([70.47, 73.11, 75.18, 77.17, 78.88, 80.73, 84.11, 87.28, 90.56, 92.32, 95.19, 101.03, 158.70, 213.48, 265.05, 312.85, 365.76], probs)],
    #                    [MultiplePeaksDist([70.83, 73.21, 75.79, 78.78, 81.66, 85.01, 89, 94.57, 98.88, 101.56, 114.15, 159.54, 202.27, 205.94, 218.64, 237.74, 273.28], probs)],
    #                    [MultiplePeaksDist([76.13, 78.05, 79.42, 82.7, 85, 86.95, 91.55, 112.28, 188.44, 229.25, 308.81, 357.62, 408.97, 426.69, 460.97, 55.78, 767.11], probs)],
    #                    [MultiplePeaksDist([63.2, 67.1, 69.99, 73.21, 76.76, 81.36, 90.03, 200.19, 306.8, 408.36, 538.51, 682.86, 921.95, 1068.16, 1278.48, 1638.96, 2175.75], probs)],
    #                    [MultiplePeaksDist([72.69, 75.77, 77.89, 79.56, 82.38, 84.92, 87.28, 91.72, 94.71, 97.25, 101.06, 136.57, 208.79, 256.96, 314.61, 378.15, 436.13], probs)],
    #                    [MultiplePeaksDist([71.03, 73.39, 75.98, 79.36, 82.76, 86.31, 90.84, 98.23, 106.84, 148.47, 190.98, 206.3, 236.09, 261.79, 308.33, 409.72, 726.84], probs)],
    #                    [MultiplePeaksDist([76.72, 78.64, 80.66, 84.31, 86.39, 90.37, 97.03, 157.34, 203.34, 213.59, 239.32, 271.2, 335.75, 358.25, 406.39, 457.2, 485.39], probs)],
    #                    [MultiplePeaksDist([63.79, 68.45, 71.1, 75.06, 78.73, 83.83, 92.13, 166.13, 204.41, 221.01, 287.43, 382.7, 488.86, 553.11, 625.75, 795.19, 1125.66], probs)],
    #                    [MultiplePeaksDist([73.82, 77.1, 79.06, 81.62, 84.36, 86.71, 90.75, 96.97, 119.79, 161.56, 206.96, 255.56, 307.25, 345.06, 394.15, 432.4, 575.09], probs)],
    #                    [MultiplePeaksDist([71.27, 73.75, 76.56, 80.15, 83.66, 87.73, 93.53, 108.07, 195.05, 205.04, 220.86, 265.23, 385.24, 428.42, 525.87, 782.4, 1368.71], probs)]]
    
    # time_generators = [[MultiplePeaksDist([50, 500], [0.9, 0.1]), MultiplePeaksDist([50, 200], [0.9, 0.1])]]
    # cluster_dists = [[0.5, 0.5]]
    # sets_names = ['two_data_storage_clusters']
    # sets_names = ['alexa_googledns_top_1k', 'alexa_opendns_top_1k', 
    #               'umbrella_google_dns_top_1k', 'umbrella_opendns_top_1k',
    #               'alexa_googledns_10k_11k', 'alexa_opendns_10k_11k',
    #               'umbrella_googledns_10k_11k', 'umbrella_opendns_10k_11k',
    #               'alexa_opendns_100k_101k', 'alexa_opendns_100k_101k',
    #               'umbrella_googledns_100k_101k', 'umbrella_opendns_100k_101k']
    
    time_generators = [[MultiplePeaksDist([50, 150], [0.9, 0.1]), MultiplePeaksDist([50, 150 * factor], [0.9, 0.1])] 
                       for factor in np.arange(10, 20.1, 1)]
    cluster_dists = [[0.5, 0.5] for factor in np.arange(10, 20.1, 1)]
    sets_names = [f'diff_factor_{factor}' for factor in np.arange(10, 20.1, 1)]
    
    if (not (len(time_generators) == len(cluster_dists))) or (not (sets_names is not None and len(time_generators) == len(sets_names))):
        raise ValueError(f'Number of cluster dist configurations: {len(cluster_dists)} and number of time generators configurations: {len(time_generators)}, or number of sets names mismatch')
    
    for i in range(len(cluster_dists)):
        verifyDists(cluster_dists[i], len(time_generators[i]))
        
    input_files_paths = [f for f in listdir(INPUT_DIR)]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    with Timer():
        for i in tqdm.trange(len(time_generators), colour='magenta', leave=False):
            addDelayAndWriteToFile(input_files_paths, time_generators[i], cluster_dists[i], verbose=args.verbose, compress=args.compress, set_name=sets_names[i])
            
if __name__ == '__main__':
    main()
