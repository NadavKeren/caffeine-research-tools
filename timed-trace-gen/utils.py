import tarfile

import numpy as np 
from typing import List, Dict

from json import dump
from math import inf


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
