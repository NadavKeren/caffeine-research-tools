import numpy as np 

import argparse
import tqdm
import re

from os.path import *
from os import path, listdir, makedirs

from typing import List, Dict

from itertools import chain, islice

from utils import *
from xxhash import xxh3_64_intdigest


class OccurenceDist():
    SIZE = 1000000

    __slots__ = 'index', 'gen_values', '_low', '_high'
    def __init__(self, low: int, high: int):
        self._low = low
        self._high = high
        
        self.refill_values()
        
        
    def refill_values(self):
        self.index = 0
        self.gen_values = np.random.randint(self._low, self._high, size=OccurenceDist.SIZE)


def addDelayAndWriteToFile(input_path: str, output_path: str, fname: str, key_base: int, 
                           set_name: str, low_occ: int, high_occ: int, seed: int):
    if low_occ >= high_occ:
        raise ValueError(f"Bad occurences limits provided: [{low_occ} {high_occ})")

    occurences = OccurenceDist(low_occ, high_occ)

    latency_values = np.array([])
    
    num_of_lines = 0
    dist_100 = 0
    dist_1000 = 0 # just sanity check
    
    output_file_name = f'{output_path}/{set_name}'
    
    with open(f'{output_file_name}.trace', 'w') as outputFile, open(f'{input_path}/{fname}') as inputFile:
        BATCH_SIZE = 10000
        lines = [line for line in islice(inputFile, 0, BATCH_SIZE)]
        num_of_lines += len(lines)
        
        while (lines):
            current_file_latencies = np.zeros(len(lines))
            
            for idx in range(len(lines)):
                timestamp, key = lines[idx].split(' ')
                key = int(key.strip(' \n'), key_base)
                
                """
                Here, using the fields instead of functions in order to reduce the call time.
                Moreover, the usage of batches lowers the computation siginificantly!
                """
                num_of_occurences = occurences.gen_values[occurences.index]
                occurences.index += 1
                if occurences.index >= OccurenceDist.SIZE:
                    occurences.refill_values()
                
                for i in range(num_of_occurences):
                    gen_key = key * high_occ + i
                    if xxh3_64_intdigest(str(gen_key), seed=seed) & 1 == 0: # * This should be 50%
                        latency = 100
                        dist_100 += 1
                    else:
                        latency = 1000
                        dist_1000 += 1

                    outputFile.write(f'{timestamp} {gen_key} 0 {latency}\n')

            
            lines = [line for line in islice(inputFile, 0, BATCH_SIZE)]
        
        total = dist_100 + dist_1000
        print(f'Dist: 100: {dist_100 / total:.2f}\t1000: {dist_1000 / total:.2f}') # sanity check, this should be approx 50-50
    

def get_trace_name(fname: str):
    name, *_ = re.findall('Trace0[0-9][0-9]', fname)
    
    return name.lower()


        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-i', '--input-dir', help='The processed files path', type=str, default=None)
    parser.add_argument('-o', '--output-dir', help='The path for the newly created files', type=str, default=None)
    parser.add_argument('-b', '--key-base', help='The base of the key string', type=int, default=10)
    parser.add_argument('--lower', help='The lower bound of the item multipliction', type=int, default=10)
    parser.add_argument('--upper', help='The upper bound of the item multipliction', type=int, default=20)
    
    args = parser.parse_args()
    
    print(f'Given args: {str(args)}')
    
    INPUT_DIR = args.input_dir if args.input_dir else './processed'
    OUTPUT_DIR = args.output_dir if args.output_dir else './out_latencies'
    

    seeds = {'trace018' : 2867, 'trace005' : 22874, 'trace000' : 36661, 'trace045' : 4150,
             'trace036' : 45755, 'trace012' : 32153, 'trace024' : 23516, 'trace031' : 38080,
             'trace049' : 57461, 'trace034' : 33022, 'trace044' : 7033, 'trace029' : 38573,
             'trace010' : 43215, 'financial1' : 282879, 'financial2' : 940359, 'websearch1': 726598,
             'websearch2' : 31069, 'websearch3' : 273312}
    input_files_paths = [f for f in listdir(INPUT_DIR)]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    assert args.lower < args.upper, "The upper bound should be bigger than the lower bound"
    
    with Timer():
        for file, _ in zip(input_files_paths, tqdm.tqdm(range(len(input_files_paths)), colour='yellow', leave=False)):
            trace_name = get_trace_name(file)
            seed = seeds[trace_name]
            set_name = f'IBMOS-{trace_name}-M-{args.lower}-{args.upper}-L'

            addDelayAndWriteToFile(INPUT_DIR, OUTPUT_DIR, file, args.key_base, 
                                   set_name, args.lower, args.upper, seed)


if __name__ == '__main__':
    main()
