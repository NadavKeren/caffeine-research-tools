import argparse
import re
import subprocess
import json

from rich import pretty, print
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from pathlib import Path

from typing import List, Dict, Any

from itertools import islice
from xxhash import xxh3_64_intdigest

from common_data import seeds
from latency_generators import NormalDist, UniformDist, MultiplePeaksDist, SingleValueDist, RANDOM_BATCH_SIZE

pretty.install()

weights_sum = 0
hash_seed = 830981


def compress_file_xz(file_path: Path, progress: Progress | None = None) -> None:
    try:
        if progress:
            progress.console.print(f'[bold #F3DFC1]Compressing {file_path.name}...')

        subprocess.run(
            ['xz', '-T0', '-z', file_path],
            capture_output=True,
            text=True,
            check=True
        )

        if progress:
            progress.console.print(f'[bold #DDBEA8]{file_path} compressed successfully.')

    except subprocess.CalledProcessError as e:
        if progress:
            progress.console.print(f'[red bold]Error compressing {file_path}: {e.stderr}')
        else:
            print(f'[red bold]Error compressing {file_path}: {e.stderr}')
    except FileNotFoundError:
        error_msg = '[red bold]xz command not found. Please install xz-utils (apt-get install xz-utils or yum install xz)'
        if progress:
            progress.console.print(error_msg)
        else:
            print(error_msg)


def calculate_sum_of_dists(cluster_dist: List[int]) -> None:
    global weights_sum
    weights_sum = 0
    for weight in cluster_dist:
        weights_sum += weight
    
    if weights_sum <= 1:
        raise ValueError("Weights should be integers")


def choose_dist(key : int | str, cluster_dists: List[int]) -> int:
    hashed_key = key % weights_sum # since weight_sum > 0 this is also positive number
    weights_acc = 0
    for idx, weight in enumerate(cluster_dists):
        weights_acc += weight
        if (hashed_key <= weights_acc):
            return idx
        
    print(f'[red bold]Error: no cluster chosen for {key}')
    exit(1)
    

def create_distribution_from_config(dist_config: Dict[str, Any], seed: int):
    dist_type = dist_config.get('type', '').lower()

    if dist_type == 'normal':
        mean = dist_config['mean']
        std_dev = dist_config['std_dev']
        return NormalDist(mean, std_dev, seed)

    elif dist_type == 'uniform':
        low = dist_config['low']
        high = dist_config['high']
        return UniformDist(low, high)

    elif dist_type == 'multiplepeaks' or dist_type == 'peaks':
        values = dist_config['values']
        probs = dist_config['probs']
        return MultiplePeaksDist(values, probs)

    elif dist_type == 'single' or dist_type == 'constant':
        value = dist_config['value']
        return SingleValueDist(value)

    else:
        raise ValueError(f"Unknown distribution type: {dist_type}")


def load_distributions_from_config(config_path: Path, seed: int) -> List[tuple]:
    with config_path.open('r') as f:
        config = json.load(f)

    distributions = []
    for dist_set in config.get('distributions', []):
        generators = []
        for gen_config in dist_set['generators']:
            generators.append(create_distribution_from_config(gen_config, seed))

        weights = dist_set['weights']
        distributions.append((generators, weights))

    return distributions


def addDelayAndWriteToFile(input_path: Path, output_path: Path, time_generators: List, cluster_dists: List[int],
                           progress: Progress, verbose: bool, set_name: str, time_multiplier: int = 1, compress: bool = False) -> None:
    calculate_sum_of_dists(cluster_dists)
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
                    key = xxh3_64_intdigest(key, seed=hash_seed)
                    
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
                    
                    
                    outputFile.write(f'{timestamp} {key} {miss_penalty}\n')
                
                lines = [line for line in islice(inputFile, 0, BATCH_SIZE)]
                num_of_lines += len(lines)
            
                if verbose and num_of_lines % 1_000_000 == 0:
                    progress.console.print(f'[dark_orange]Added latencies to [cyan bold]{num_of_lines:,}')
                    progress.console.print(f'[cyan]Dists: {chosen_dist_counter}')

    progress.console.print(f'[bold #ccd8ab]Processed f{input_path.name} with {num_of_lines:,}, splitting to {chosen_dist_counter}')

    if compress:
        compress_file_xz(output_file, progress=progress)

        
def main():
    parser = argparse.ArgumentParser(description='Add latency information to parsed trace files')

    parser.add_argument('-c', '--compress', help="Compress the newly created traces files", action='store_true')
    parser.add_argument('-v', '--verbose', help='Prints the time elapsed and number of unique entries for each file, in addition to the progress bar', action='store_true')
    parser.add_argument('-i', '--input-dir', help='The processed files dir path', type=str, default=None)
    parser.add_argument('-o', '--output-dir', help='The path for the newly created files, default = (input_dir)/out_latencies', type=str, default=None)
    parser.add_argument('-d', '--distribution-config', help='Path to JSON config file for latency distributions (required)', type=str, required=True)

    args = parser.parse_args()

    print(f'Given args: {str(args)}')

    INPUT_DIR = Path(args.input_dir if args.input_dir else './processed')
    OUTPUT_DIR = Path(args.output_dir if args.output_dir else str(INPUT_DIR.resolve()) + './out_latencies')

    config_path = Path(args.distribution_config)
    if not config_path.exists():
        print(f'[red bold]Error: Config file {config_path} does not exist')
        exit(1)

    print(f'Input dir: {str(INPUT_DIR.resolve())} Output dir: {str(OUTPUT_DIR.resolve())}')
    print(f'Distribution config: {str(config_path.resolve())}')

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
            trace_name = file.stem.lower()
            time_multiplier = 1
            if (file.stem.startswith("twitter") or file.stem.lower().startswith("meta")):
                time_multiplier = 1000

            progress.console.print(f'[cyan]Chose {time_multiplier} as time multiplier')
            seed = seeds[trace_name]
            set_name = f'IBMOS-{trace_name}-' if "IBMObjectStore" in file.stem else trace_name

            progress.console.print(f'[cyan]Loading distributions from config: {config_path}')
            dists = load_distributions_from_config(config_path, seed)

            gen_progress = progress.add_task('[bold #adc178]Configuration', total=len(dists), start=True)
            for (dist_gens, probs) in dists:
                suffix = '-'.join(f'{chr(ord('A') + i)}-{repr(dist)}' for i, dist in enumerate(dist_gens))
                progress.console.print(f'Dists: {' '.join([repr(dist) for dist in dist_gens])}')

                addDelayAndWriteToFile(file, OUTPUT_DIR, dist_gens,
                                       probs, progress=progress, verbose=args.verbose,
                                       set_name=set_name + '-' + suffix, time_multiplier=time_multiplier,
                                       compress=args.compress)
                progress.update(gen_progress, advance=1)

            progress.remove_task(gen_progress)
            progress.update(file_progress, advance=1)

    print(f'[green]Done appending times to [cyan]{len(input_files_paths)}[green] files')
            

 
if __name__ == '__main__':
    main()
