from datetime import timedelta
import time
import tarfile

import numpy as np 
from typing import Generator, List, Dict

from json import dump
from math import inf


class Colors():
    reset='\033[0m'
    bold='\033[01m'
    red='\033[31m'
    green='\033[32m'
    orange='\033[33m'
    blue='\033[34m'
    purple='\033[35m'
    cyan='\033[36m'
    lightgrey='\033[37m'
    
    darkgrey='\033[90m'
    lightred='\033[91m'
    lightgreen='\033[92m'
    yellow='\033[93m'
    lightblue='\033[94m'
    pink='\033[95m'
    lightcyan='\033[96m'

class Timer():
    def __init__(self, msg: str = None):
        self._start_time = None
        self._end_time = None
        self._msg = msg
    
    def __enter__(self):
        self._start_time = time.time()
        return self
    
    
    def __exit__(self, *exec_info):
        self._end_time = time.time()
        diff = self._end_time - self._start_time
        delta = timedelta(seconds=diff).total_seconds()
        minutes = int(delta) // 60
        seconds = delta - minutes * 60
        
        printed_str = None
        if self._msg is None:
            printed_str = f'{Colors.bold}{Colors.purple}Elapsed time: '
        else:
            printed_str = f'{Colors.bold}{Colors.purple}Elapsed time of {Colors.green}{self._msg}{Colors.purple}: '
        
        print(f'{printed_str}{Colors.cyan}{minutes}:{seconds:04.2f}{Colors.reset}')
 
 
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
        
        
def compressTrace(output_file_name: str, should_remove=False):
    with Timer(msg='compression') as t:
        with tarfile.open(f'{output_file_name}.xz', mode="w:xz") as tar:
            tar.add(f'{output_file_name}.trace')
            
    if should_remove:
        run(['rm', f'{output_file_name}.trace'])
