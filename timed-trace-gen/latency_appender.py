import numpy as np 

import argparse
import datetime
import re

from rich import pretty, print
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from os.path import *
from os import path, listdir, makedirs
from subprocess import run

from typing import List, Dict

from itertools import chain, islice

from utils import *
from common_data import seeds
from latency_generators import *


def addDelayAndWriteToFile(input_path: str, output_path: str, fname: str, key_base: int, time_generators: List, cluster_dists: List[float], 
                           progress: Progress, verbose: bool, compress: bool, gen_timestamps: bool, hit_penalty=0, set_name: str = None, seed: int = None):
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
            
            if verbose:
                progress.console.print(f'[dark_orange bold]Added latencies to [cyan bold]{len(lines):,} '
                                     + f'[dark_orange bold] lines with [cyan bold]{len(keysTimeDistDict):,}'
                                     + f'[dark_orange bold] unique entries so far')
    
    writeMetaData(output_file_name, time_generators, cluster_dists, latency_values, keysTimeDistDict)
    
    if (compress):
        compressTrace(output_file_name, should_remove=True)
        
    
def get_trace_name(fname: str):
    if ("IBMObjectStore" in fname):
        name = re.findall('Trace0[0-9][0-9]', fname)
        name = name[0]
    elif ("Finacial" in fname or "WebSearch" in fname):
        name = fname.rstrip('.spc')
    elif fname.endswith(".trace"):
        name = fname.rstrip(".trace")
    
    return name.lower()


        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-c', '--compress', help="Compress the newly created traces files", action='store_true')
    parser.add_argument('-v', '--verbose', help='Prints the time elapsed and number of unique entries for each file, in addition to the progress bar', action='store_true')
    parser.add_argument('-i', '--input-dir', help='The processed files path', type=str, default=None)
    parser.add_argument('-o', '--output-dir', help='The path for the newly created files', type=str, default=None)
    parser.add_argument('-t', '--contains-timestamps', help='Toggle whether the file contains timestamps', action='store_true')
    parser.add_argument('-b', '--key-base', help='The base of the key string', type=int, default=10)
    
    args = parser.parse_args()
    
    print(f'Given args: {str(args)}')
    
    INPUT_DIR = args.input_dir if args.input_dir else './processed'
    OUTPUT_DIR = args.output_dir if args.output_dir else './out_latencies'
    
    #([(120, 12.16), (40, 6.08)], [0.5, 0.5]),
    dists = [([(120, 12.16), (340, 14), (675, 15.2)], [0.34, 0.33, 0.33])]
    
    input_files_paths = [f for f in listdir(INPUT_DIR)]

    
    makedirs(OUTPUT_DIR, exist_ok=True)
    print(f'Processing files: {input_files_paths} for dists: {dists}')
    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  SpinnerColumn()) as progress:
        gen_progress = progress.add_task('[bold]Distribution', total=len(dists), start=True)
        for (params, probs) in dists:
            suffix = '-'.join(f'{chr(ord('A') + i)}-{int(mu - 1.6449*sigma)}-{int(mu + 1.6449*sigma)}' for i, (mu, sigma) in enumerate(params))

            file_progress = progress.add_task('[bold #adc178]File progress', total=len(input_files_paths), start=True)
            
            for file in input_files_paths:
                progress.console.print(f'Processing {file}')
                trace_name = get_trace_name(file)
                seed = seeds[trace_name]
                set_name = f'IBMOS-{trace_name}-' if "IBMObjectStore" in file else trace_name
                
                dist_gen = [NormalDist(mu, sigma, seed) for (mu, sigma) in params]
                addDelayAndWriteToFile(INPUT_DIR, OUTPUT_DIR, file, args.key_base, dist_gen, 
                                    probs, gen_timestamps=not args.contains_timestamps, progress=progress, verbose=args.verbose, 
                                    compress=args.compress, set_name=set_name + suffix, seed=seed)
                progress.update(file_progress, update=1)
            
            progress.update(gen_progress, update=1)

 
if __name__ == '__main__':
    main()
