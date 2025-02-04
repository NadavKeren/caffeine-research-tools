import argparse
import simulatools
import re
from os import urandom
from pathlib import Path
import json

from rich import pretty, print
from rich.console import Console
from rich.progress import Progress

filepath = Path(__file__) 
current_dir = filepath.parent
filepath = current_dir.resolve()
result_dir = current_dir / 'results'
result_dir.mkdir(exist_ok=True)

with open(current_dir / 'conf.json') as conf_file:
    local_conf = json.load(conf_file)
caffeine_root = local_conf['caffeine_root']
resources = local_conf['resources'] if local_conf['resources'] != '' else caffeine_root
TRACES_DIR = f'{resources}'


pretty.install()
console = Console()

NUM_OF_QUANTA = 16

# SIZES = {'trace010' : 2 ** 10, 'trace024' : 2 ** 9, 'trace031' : 2 ** 16,
#          'trace045' : 2 ** 12, 'trace034' : 2 ** 14, 'trace029' : 2 ** 9,
#          'trace012' : 2 ** 10}

PIPELINE_SETTINGS_WITHOUT_QUOTA = {"pipeline.num-of-blocks" : 3,
                                   "pipeline.blocks.0.type": "LRU",
                                   "pipeline.blocks.0.decay-factor" : 1, 
                                   "pipeline.blocks.0.max-lists" : 10,
                                   "pipeline.blocks.1.type": "LFU",
                                   "pipeline.blocks.1.decay-factor" : 1, 
                                   "pipeline.blocks.1.max-lists" : 10,
                                   "pipeline.blocks.2.type": "BC",
                                   "pipeline.burst.aging-window-size" : 50, 
                                   "pipeline.burst.age-smoothing" : 0.0025, 
                                   "pipeline.burst.number-of-partitions" : 4, 
                                   "pipeline.burst.type" : "sketch", 
                                   "pipeline.burst.sketch.eps" : 0.0001, 
                                   "pipeline.burst.sketch.confidence" : 0.99}

PIPELINE_EQUAL_START_SETTINGS = {**PIPELINE_SETTINGS_WITHOUT_QUOTA,
                                 "pipeline.blocks.0.quota": 5, 
                                 "pipeline.blocks.1.quota": 6, 
                                 "pipeline.blocks.2.quota": 5}

PIPELINE_LRU_START_SETTINGS = {**PIPELINE_SETTINGS_WITHOUT_QUOTA,
                               "pipeline.blocks.0.quota": 14, 
                               "pipeline.blocks.1.quota": 1,
                               "pipeline.blocks.2.quota": 1}

PIPELINE_SETTINGS_WITHOUT_BURST = {"pipeline.num-of-blocks" : 2,
                                   "pipeline.blocks.0.type": "LRU",
                                   "pipeline.blocks.0.decay-factor" : 1, 
                                   "pipeline.blocks.0.max-lists" : 10,
                                   "pipeline.blocks.0.quota": 8, 
                                   "pipeline.blocks.1.type": "LFU",
                                   "pipeline.blocks.1.decay-factor" : 1, 
                                   "pipeline.blocks.1.max-lists" : 10,
                                   "pipeline.blocks.1.quota": 8}

PIPELINE_LRU_ONLY = {"pipeline.num-of-blocks" : 1, 
                     "pipeline.num-of-quanta" : 16,
                     "pipeline.blocks.0.type": "LRU",
                     "pipeline.blocks.0.quota": 16, 
                     "pipeline.blocks.0.decay-factor" : 1, 
                     "pipeline.blocks.0.max-lists" : 10}

PIPELINE_LFU_ONLY = {"pipeline.num-of-blocks" : 1, 
                     "pipeline.num-of-quanta" : 16,
                     "pipeline.blocks.0.type": "LFU",
                     "pipeline.blocks.0.quota": 16, 
                     "pipeline.blocks.0.decay-factor" : 1, 
                     "pipeline.blocks.0.max-lists" : 10}


PIPELINE_BC_ONLY = {"pipeline.num-of-blocks" : 1, 
                    "pipeline.num-of-quanta" : 16,
                    "pipeline.blocks.0.type": "BC",
                    "pipeline.blocks.0.quota": 16, 
                    "pipeline.burst.aging-window-size" : 50, 
                    "pipeline.burst.age-smoothing" : 0.0025, 
                    "pipeline.burst.number-of-partitions" : 4, 
                    "pipeline.burst.type" : "sketch", 
                    "pipeline.burst.sketch.eps" : 0.0001, 
                    "pipeline.burst.sketch.confidence" : 0.99}

SEED_PATH = 'random-seed'


SETTINGS = {"pipeline.num-of-quanta" : NUM_OF_QUANTA,
            "pipeline.burst.aging-window-size" : 50, 
            "pipeline.burst.age-smoothing" : 0.0025, 
            "pipeline.burst.number-of-partitions" : 4, 
            "pipeline.burst.type" : "sketch", 
            "pipeline.burst.sketch.eps" : 0.0001, 
            "pipeline.burst.sketch.confidence" : 0.99,
            'full-ghost-hill-climber.adaption-multiplier' : 10}

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


def get_trace_name(fname: str):
    temp_fname = fname.lower()
    name = re.findall('trace0[0-9][0-9]', temp_fname)
    
    return name[0]


def run_test(fname: str, trace_name: str, cache_size: int, output_filename : str,
             algorithm : str, should_keep_dump : bool = True, additional_settings = None,
             name = None, additional_csv_data = None,
             progress_console = None) -> None:
    if progress_console:
        progress_console.log(f'[bold #a98467]Running {algorithm} on trace: {trace_name}, size: {cache_size}' + f' Name: {name}' if name is not None else "")
    else:
        console.log(f'[bold #a98467]Running {algorithm} on trace: {trace_name}, size: {cache_size}' + f' Name: {name}' if name is not None else "")
    
    if (Path(f'./results/{output_filename}.csv').exists()): # * Skipping tests with existing results        
        return
    
    settings = SETTINGS if additional_settings is None else {**SETTINGS, **additional_settings}
        
    single_run_result = simulatools.single_run(algorithm, trace_files=[fname], trace_folder='latency', 
                                                trace_format='LATENCY', size=cache_size,
                                                additional_settings=settings,
                                                name=f'{algorithm}-{trace_name}' if name is None else f'{algorithm}-{trace_name}-{name}',
                                                save = False, verbose = False)
    
    if (single_run_result is False):
        if progress_console:
            progress_console.log(f'[bold red]Error in {fname}: exiting')
        else:
            console.log(f'[bold red]Error in {fname}: exiting')
        
        exit(1)
    else:                    
        single_run_result['Cache Size'] = cache_size
        single_run_result['Trace'] = trace_name
        
        if additional_csv_data is not None:
            for key, value in additional_csv_data.items():
                single_run_result[key] = value
        
        single_run_result.to_csv(f'./results/{output_filename}.csv')
        if progress_console:
            progress_console.log(f"[bold #ffd166]Avg. Pen. {int(single_run_result['Average Penalty'].iloc[0])}")
        else:
            console.log(f"[bold #ffd166]Avg. Pen. {int(single_run_result['Average Penalty'].iloc[0])}")
        
        if should_keep_dump:
            dump_path = Path(f'{caffeine_root}')
            quota_files = [file.resolve() for file in dump_path.rglob('*.quota-dump')]
            if not len(quota_files) == 1:
                if progress_console:
                    progress_console.log(f"[bold red]Wrong number of dump-files found: {len(quota_files)}")
                else:
                    console.print(f"[bold red]Wrong number of dump-files found: {len(quota_files)}")
                    
                raise AssertionError()
            
            dumpfile = quota_files[0]
            dumpfile.rename(result_dir / f'{output_filename}.quota-dump')


def run_full_ghost(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
    
    csv_filename = f'FGHC-{trace_name}-extended-{cache_size}'
    run_test(fname, trace_name, cache_size, csv_filename, 'full_ghost', 
                name='FGHC', additional_settings={**PIPELINE_LRU_START_SETTINGS, 
                                                  **SIZE_SETTINGS})
    
    csv_filename = f'FGHC-RF-{trace_name}-extended-{cache_size}'
    run_test(fname, trace_name, cache_size, csv_filename, 'full_ghost', 
                name='FGHC-RF', additional_settings={**PIPELINE_SETTINGS_WITHOUT_BURST, 
                                                     **SIZE_SETTINGS})


def run_sampled(fname: str, trace_name: str, cache_size: int, round: int, seed: int, progress: Progress) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]

    sample_progress = progress.add_task('[bold #bedcfe]Current round', total=6, start=True)
    for sample_rate in range(1, 7):
        if (int(quantum_size) >> sample_rate > 0):
            SAMPLE_SETTINGS = {'sampled-hill-climber.sample-order-factor' : sample_rate, 
                            'sampled-hill-climber.adaption-multiplier' : 10}
            
            SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
            
            csv_filename = f'sampled-O{sample_rate}-{trace_name}-extended-{cache_size}-R{round}'
            run_test(fname, trace_name, cache_size, csv_filename, 'sampled_ghost', 
                    name=f'extended-O{sample_rate}', additional_settings={**PIPELINE_LRU_START_SETTINGS, 
                                                                        **SAMPLE_SETTINGS, 
                                                                        **SIZE_SETTINGS, 
                                                                        SEED_PATH: seed},
                    additional_csv_data={'Round' : round, 'Seed': seed}, console=progress.console)
        progress.update(sample_progress, advance=1)
    progress.remove_task(sample_progress)


def run_random_hill_climber(fname: str, trace_name: str, cache_size: int, round: int, seed: int, progress: Progress) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}

    csv_filename = f'RHC-{trace_name}-extended-{cache_size}'
    run_test(fname, trace_name, cache_size, csv_filename, 'random_climber', 
            name='RHC', additional_settings={**PIPELINE_LRU_START_SETTINGS, **SIZE_SETTINGS, SEED_PATH: seed}, 
            additional_csv_data={'Round' : round, 'Seed': seed}, console=progress.console)
    

def run_all_simple(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}
    
    csv_filename = f'LRU-{trace_name}-extended-{cache_size}'
    run_test(fname, trace_name, cache_size, csv_filename, 'pipeline', 
            name='LRU', additional_settings={**PIPELINE_LRU_ONLY, **SIZE_SETTINGS}, should_keep_dump=False)

    csv_filename = f'LFU-{trace_name}-extended-{cache_size}'
    run_test(fname, trace_name, cache_size, csv_filename, 'pipeline', 
            name='LFU', additional_settings={**PIPELINE_LFU_ONLY, **SIZE_SETTINGS}, should_keep_dump=False)
    
    csv_filename = f'BC-{trace_name}-extended-{cache_size}'
    run_test(fname, trace_name, cache_size, csv_filename, 'pipeline', 
            name='BC', additional_settings={**PIPELINE_BC_ONLY, **SIZE_SETTINGS}, should_keep_dump=False)

def run_grid_search(fname: str, trace_name: str, cache_size: int) -> None:
    with Progress() as progress:
        lru_progress = progress.add_task('[bold #adc178]LRU quota', total=16, start=True)
        lfu_progress = progress.add_task('[bold #bedcfe]LFU quota', total=16, start=True)
        for lru_size in range(NUM_OF_QUANTA + 1):
            for lfu_size in range(NUM_OF_QUANTA - lru_size + 1):
                bc_size = NUM_OF_QUANTA - (lru_size + lfu_size)
                csv_filename = f'static-{lru_size}-{lfu_size}-{bc_size}-extended-{cache_size}'
                run_test(fname, trace_name, cache_size, csv_filename, 'pipeline',
                         name=f"{lru_size}-{lfu_size}-{bc_size}", 
                         additional_settings={**PIPELINE_SETTINGS_WITHOUT_QUOTA,
                                              "pipeline.blocks.0.quota": lru_size, 
                                              "pipeline.blocks.1.quota": lfu_size,
                                              "pipeline.blocks.2.quota": bc_size},
                         should_keep_dump=False,
                         additional_csv_data={'LRU Size': lru_size, 'LFU Size': lfu_size, 'BC Size': bc_size},
                         console=progress.console)
                progress.update(lfu_progress, advance=1)
            
            progress.update(lru_progress, advance=1)
            progress.reset(lfu_progress, total=(NUM_OF_QUANTA - lru_size - 1))


def run_adaptive_CA(fname: str, trace_name: str, cache_size: int, round: int, progress: Progress) -> None:
    csv_filename = f'ACA-{trace_name}-extended-{cache_size}-R{round}'
    run_test(fname, trace_name, cache_size, csv_filename, 'adaptive_ca',
             should_keep_dump=False, console=progress.console)
    
    
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input', help="The input trace path", required=True)
    parser.add_argument('--rounds', help="number of round to perform", required=False, type=int)
    parser.add_argument('--round-index-start', help="The starting index for the round numbers", required=False, type=int, default=0)
    parser.add_argument('--run-shc', help="Run rounds of Sample Hill Climber", action='store_true', required=False)
    parser.add_argument('--run-aca', help="Run rounds of the Adaptive Cost-Aware Window-TinyLFU", action='store_true', required=False)
    parser.add_argument('--run-base', help="Run the baseline test of FGHC", action='store_true', required=False)
    parser.add_argument('--run-grid-search', help="Run grid search for finding the optimal static configuration", action='store_true', required=False)
    
    args = parser.parse_args()
    
    console.print(f'[bold]Running with args:[/bold]\n{args}')
    
    file = args.input

    console.print(f'Testing file: {file}')

    # trace_name = get_trace_name(file)
    # cache_size = SIZES.get(trace_name) * 10
    trace_name = "trace024-010-024"
    cache_size = 5120

    
    if args.run_base:
        run_full_ghost(file, trace_name, cache_size)
        run_all_simple(file, trace_name, cache_size)

    if args.rounds is not None:
        with Progress() as progress:
            round_progress = progress.add_task('[bold #adc178]Rounds', total=args.rounds, start=True)
            for round in range(args.rounds):
                if args.run_shc:
                    seed = abs(int.from_bytes(urandom(4), 'big', signed=True))
                    progress.console.log(f"Starting round {round + 1} of {args.rounds}: {100.0 * round / args.rounds}%, seed: {seed}", style='bold #adc178')

                    run_sampled(file, trace_name, cache_size, args.round_index_start + round + 1, seed, progress=progress)
                
                if args.run_aca:
                    run_adaptive_CA(file, trace_name, cache_size, args.round_index_start + round + 1, progress=progress)
                
                progress.update(round_progress, advance=1)
                # run_random_hill_climber(file, trace_name, cache_size, args.round_index_start + round + 1, seed, progress=progress)
            
    if args.run_grid_search:
        run_grid_search(file, trace_name, cache_size)
                

            
    
    console.log(f"[bold #a3b18a]Done\n#####################\n\n")

if __name__ == "__main__":
    main()
