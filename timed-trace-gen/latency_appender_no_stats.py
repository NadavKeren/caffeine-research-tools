import numpy as np 

import argparse
import datetime
import re

from rich import pretty, print
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from pathlib import Path

from typing import List, Dict

from itertools import chain, islice
from xxhash import xxh3_64_intdigest

from utils import *
from common_data import seeds
from latency_generators import *

pretty.install()

weights_sum = 0
hash_seed = 830981

def calculate_sum_of_dists(cluster_dist: List[int]) -> None:
    global weights_sum
    weights_sum = 0
    for weight in cluster_dist:
        weights_sum += weight
    
    if weights_sum <= 1:
        raise ValueError(f"Weights should be integers")


def choose_dist(key : int | str, cluster_dists: List[int]) -> int:
    hashed_key = xxh3_64_intdigest(key, seed=hash_seed) % weights_sum # since weight_sum > 0 this is also positive number
    weights_acc = 0
    for idx, weight in enumerate(cluster_dists):
        weights_acc += weight
        if (hashed_key <= weights_acc):
            return idx
        
    print(f'[red bold]Error: no cluster chosen for {key}')
    exit(1)
    

def addDelayAndWriteToFile(input_path: Path, output_path: Path, key_base: int, time_generators: List, cluster_dists: List[int], 
                           progress: Progress, verbose: bool, set_name: str, time_multiplier: int = 1) -> None:
    calculate_sum_of_dists(cluster_dists)
    current_time = datetime.datetime.now()
    timestamp = 1
    num_of_lines = 0
    chosen_dist_counter = [0] * len(cluster_dists)
    
    output_file : Path = output_path / f'{set_name}.trace'
    if output_file.exists():
        return
    
    with output_file.open('w') as outputFile:
        with input_path.open('r') as inputFile:
            BATCH_SIZE = 10_000
            lines = [line for line in islice(inputFile, 0, BATCH_SIZE)]
            num_of_lines += len(lines)
            
            while (lines):
                for idx in range(len(lines)):
                    line = lines[idx]
                    timestamp, key = line.split(' ')
                    timestamp = int(timestamp) * time_multiplier
                    key = key.strip(' \n')
                    
                    chosen_dist = choose_dist(key, cluster_dists)
                    chosen_gen = time_generators[chosen_dist]
                    chosen_dist_counter[chosen_dist] += 1
                                     
                    """
                    Here, using the fields instead of functions in order to reduce the call time.
                    Moreover, the usage of batches lowers the computation time by 90%!
                    """
                    if chosen_gen.index >= RANDOM_BATCH_SIZE:
                        chosen_gen.refill_values()
                        
                    miss_penalty = chosen_gen.gen_values[chosen_gen.index]
                    chosen_gen.index += 1
                    
                    
                    outputFile.write(f'{timestamp} {key} 0 {miss_penalty}\n')
                
                lines = [line for line in islice(inputFile, 0, BATCH_SIZE)]
                num_of_lines += len(lines)
            
                if verbose and num_of_lines % 1_000_000 == 0:
                    progress.console.print(f'[dark_orange]Added latencies to [cyan bold]{num_of_lines:,}')
                    progress.console.print(f'[cyan]Dists: {chosen_dist_counter}')
    
    progress.console.print(f'[bold #ccd8ab]Processed f{input_path.name} with {num_of_lines:,}, splitting to {chosen_dist_counter}')
    
    
def get_trace_name(fname: str):
    name = fname
    if ("IBMObjectStore" in fname):
        name = re.findall('Trace0[0-9][0-9]', fname)
        name = name[0]
    elif (fname.lower().startswith("metakv")):
        match = re.match(r'^(metakv\d+)', fname.lower())
        name = match.group(1)
    
    print(fname)
    
    return name.lower()

        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-c', '--compress', help="Compress the newly created traces files", action='store_true')
    parser.add_argument('-v', '--verbose', help='Prints the time elapsed and number of unique entries for each file, in addition to the progress bar', action='store_true')
    parser.add_argument('-i', '--input-dir', help='The processed files path', type=str, default=None)
    parser.add_argument('-o', '--output-dir', help='The path for the newly created files', type=str, default=None)
    parser.add_argument('-b', '--key-base', help='The base of the key string', type=int, default=10)
    
    args = parser.parse_args()
    
    print(f'Given args: {str(args)}')
    
    INPUT_DIR = Path(args.input_dir if args.input_dir else './processed')
    OUTPUT_DIR = Path(args.output_dir if args.output_dir else './out_latencies')
    
    input_files_paths = list(f for f in INPUT_DIR.iterdir() if not f.is_dir())

    
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    print(f'Processing files: {input_files_paths}')
    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(),
                  TaskProgressColumn(),
                  SpinnerColumn()) as progress:
        file_progress = progress.add_task('[bold #6c7e3a]File progress', total=len(input_files_paths), start=True)
        for file in input_files_paths:
            progress.console.print(f'Processing {file}')
            trace_name = get_trace_name(file.stem)
            time_multiplier = 1
            if (file.stem.startswith("twitter") or file.stem.lower().startswith("meta")):
                time_multiplier = 1000
                
            progress.console.print(f'[cyan]Chose {time_multiplier} as time multiplier')
            seed = seeds[trace_name]
            set_name = f'IBMOS-{trace_name}-' if "IBMObjectStore" in file.stem else trace_name
            
            dists = [([NormalDist(120, 12.16, seed), NormalDist(40, 6.08, seed)], [50, 50])]
            # dists = [([NormalDist(120, 12.16, seed), NormalDist(340, 14, seed), NormalDist(675, 15.2, seed)], [34, 33, 33])]
            gen_progress = progress.add_task('[bold #adc178]Configuration', total=len(dists), start=True)
            for (dist_gens, probs) in dists:
                suffix = '-'.join(f'{chr(ord('A') + i)}-{repr(dist)}' for i, dist in enumerate(dist_gens))
                progress.console.print(f'Dists: {' '.join([repr(dist) for dist in dist_gens])}')
                
                addDelayAndWriteToFile(file, OUTPUT_DIR, args.key_base, dist_gens, 
                                       probs, progress=progress, verbose=args.verbose, 
                                       set_name=set_name + '-' + suffix, time_multiplier = time_multiplier)
                progress.update(gen_progress, advance=1)
                
            progress.remove_task(gen_progress)
            progress.update(file_progress, advance=1)
            

 
if __name__ == '__main__':
    main()
