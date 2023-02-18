import numpy as np 

import argparse
import datetime
import tqdm
import re

from os.path import *
from os import path, listdir, makedirs
from subprocess import run

from typing import List, Dict

from itertools import chain, islice

from utils import *
from latency_generators import *


def addDelayAndWriteToFile(input_path: str, output_path: str, fnames: List[str], key_base: int, time_generators: List, cluster_dists: List[float], 
                           verbose: bool, compress: bool, gen_timestamps: bool, hit_penalty=1, set_name: str = None, seed: int = None):
    if seed is not None:
        np.random.seed(seed)
    
    latency_values = np.array([])
    
    keysTimeDistDict : Dict[str, KeyInfo] = dict()
    
    current_time = datetime.datetime.now()
    timestamp = 1
    num_of_lines = 0
    
    if set_name is not None:
        output_file_name = f'{output_path}/{set_name}'
    else:
        if len(fnames) > 1:
            output_file_name = f'{output_path}/latency_{current_time.strftime("%H%M%S_%d%m%Y")}'
        else:
            output_file_name = f'{output_path}/{fnames[0]}_{current_time.strftime("%H%M%S_%d%m%Y")}'
    
    with open(f'{output_file_name}.trace', 'w') as outputFile:
        for fname, _ in zip(fnames, tqdm.tqdm(range(len(fnames)), colour='yellow', leave=False)):
            with open(f'{input_path}/{fname}') as inputFile:
                BATCH_SIZE = 10000
                lines = [line for line in islice(inputFile, 0, BATCH_SIZE)]
                num_of_lines += len(lines)
                
                while (lines):
                    current_file_latencies = np.zeros(len(lines))
                    
                    for idx in range(len(lines)):
                        line = lines[idx]
                        splitted = line.split(' ')
                        key = splitted[0] if gen_timestamps else splitted[1]
                        key = int(key.strip(' \n'), key_base)
                        
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
                        
                        if (gen_timestamps):
                            outputFile.write(f'{timestamp} {key} {hit_penalty} {delay}\n')
                            timestamp += 1
                        else:
                            timestamp = splitted[0]
                            outputFile.write(f'{timestamp} {key} {hit_penalty} {delay}\n')
                    
                    latency_values = np.concatenate((latency_values, current_file_latencies))
                    
                    lines = [line for line in islice(inputFile, 0, BATCH_SIZE)]
                    num_of_lines += len(lines)
                
                if (verbose):
                    print(f'{Colors.orange}Added latencies to {Colors.cyan}{len(lines):,} '
                        + f'{Colors.orange} lines with {Colors.cyan}{len(keysTimeDistDict):,}'
                        + f'{Colors.orange} unique entries so far{Colors.reset}')
    
    writeMetaData(output_file_name, time_generators, cluster_dists, latency_values, keysTimeDistDict)
    
    if (compress):
        compressTrace(output_file_name, should_remove=True)
        
    
def get_trace_name(fname: str):
    name = re.findall('Trace0[0-9][0-9]', fname)
    
    return name[0].lower()


        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-c', '--compress', help="Compress the newly created traces files", action='store_true')
    parser.add_argument('-v', '--verbose', help='Prints the time elapsed and number of unique entries for each file, in addition to the progress bar', action='store_true')
    parser.add_argument('-i', '--input-dir', help='The processed files path', type=str, default=None)
    parser.add_argument('-o', '--output-dir', help='The path for the newly created files', type=str, default=None)
    parser.add_argument('-t', '--contains-timestamps', help='Toggle whether the file contains timestamps', action='store_true')
    parser.add_argument('-b', '--key-base', help='The base of the key string', type=int, default=10)
    parser.add_argument('--time', type=int, required=False, help='The second dist time in two-dist generation')
    
    args = parser.parse_args()
    
    print(f'Given args: {str(args)}')
    
    INPUT_DIR = args.input_dir if args.input_dir else './processed'
    OUTPUT_DIR = args.output_dir if args.output_dir else './out_latencies'
    

    times = [args.time] if args.time is not None else [100, 200, 400, 800]
    seeds = {'trace018' : 2867, 'trace005' : 22874, 'trace000' : 36661, 'trace045' : 4150,
             'trace036' : 45755, 'trace012' : 32153, 'trace024' : 23516, 'trace031' : 38080,
             'trace049' : 57461, 'trace034' : 33022, 'trace044' : 7033, 'trace029' : 38573,
             'trace010' : 43215}
    input_files_paths = [f for f in listdir(INPUT_DIR) if 'Trace018' not in f and 'Trace005' not in f]
    input_files_with_names = [(f, time, get_trace_name(f), f'IBMOS_{get_trace_name(f)}_50_{time}') 
                              for f in input_files_paths 
                              for time in times]
        
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    with Timer():
        with tqdm.tqdm(total = len(input_files_with_names)) as progressbar:
            for (file, time, trace_name, set_name) in input_files_with_names:
                seed = seeds[trace_name]
                addDelayAndWriteToFile(INPUT_DIR, OUTPUT_DIR, [file], args.key_base, [SingleValueDist(50), SingleValueDist(time)], 
                                       [0.5, 0.5], gen_timestamps=not args.contains_timestamps, verbose=args.verbose, 
                                       compress=args.compress, set_name=set_name, seed=seed)
                progressbar.update(1)

 
if __name__ == '__main__':
    main()
