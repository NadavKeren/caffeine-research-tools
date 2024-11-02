import argparse
import simulatools
import pprint
import pickle
import re
from os import path, listdir, makedirs, urandom
from shutil import move
import datetime
import json
import tqdm

with open(path.join(path.dirname(__file__), 'conf.json')) as conf_file:
    local_conf = json.load(conf_file)
caffeine_root = local_conf['caffeine_root']
resources = local_conf['resources'] if local_conf['resources'] != '' else caffeine_root
TRACES_DIR = f'{resources}'

SIZES = {'trace010' : 2 ** 10, 'trace024' : 2 ** 9, 'trace031' : 2 ** 16,
         'trace045' : 2 ** 12, 'trace034' : 2 ** 14, 'trace029' : 2 ** 9,
         'trace012' : 2 ** 10}


PIPELINE_EQUAL_START_SETTINGS = {"pipeline.num-of-block" : 3, 
                                "pipeline.num-of-quanta" : 16,
                                "pipeline.burst.aging-window-size" : 50, 
                                "pipeline.burst.age-smoothing" : 0.0025, 
                                "pipeline.burst.number-of-partitions" : 4, 
                                "pipeline.burst.type" : "sketch", 
                                "pipeline.burst.sketch.eps" : 0.0001, 
                                "pipeline.burst.sketch.confidence" : 0.99,
                                "pipeline.blocks.0.type": "LRU",
                                "pipeline.blocks.0.quota": 5, 
                                "pipeline.blocks.0.decay-factor" : 1, 
                                "pipeline.blocks.0.max-lists" : 10,
                                "pipeline.blocks.1.type": "LFU",
                                "pipeline.blocks.1.quota": 6, 
                                "pipeline.blocks.1.decay-factor" : 1, 
                                "pipeline.blocks.1.max-lists" : 10,
                                "pipeline.blocks.2.type": "BC",
                                "pipeline.blocks.2.quota": 5}


FULL_GHOST_SETTINGS = {'full-ghost-hill-climber.adaption-multiplier' : 10}

SEED_PATH = 'random-seed'


SETTINGS = {**PIPELINE_EQUAL_START_SETTINGS, **FULL_GHOST_SETTINGS}

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


def run_test(fname: str, trace_name: str, cache_size: int, pickle_filename : str,
             algorithm : str, dump_filename : str = None, additional_settings = None, name = None, additional_pickle_data = None) -> None:
    now = datetime.datetime.now()
    print(f'{now.strftime("%H:%M:%S")}: {Colors.pink}Running {algorithm} on trace: {trace_name}, size: {cache_size}{Colors.reset}' + f' Name: {name}' if name is not None else "")
    
    if (path.isfile(f'./results/{pickle_filename}')): # * Skipping tests with existing results        
        return
    
    settings = SETTINGS if additional_settings is None else {**SETTINGS, **additional_settings}
        
    single_run_result = simulatools.single_run(algorithm, trace_files=[fname], trace_folder='latency', 
                                                trace_format='LATENCY', size=cache_size,
                                                additional_settings=settings,
                                                name=f'{algorithm}-{trace_name}' if name is None else f'{algorithm}-{trace_name}-{name}',
                                                save = False, verbose = False)
    
    if (single_run_result is False):
        print(f'{Colors.bold}{Colors.red}Error in {fname}: exiting{Colors.reset}')
        exit(1)
    else:                    
        single_run_result['Cache Size'] = cache_size
        single_run_result['Trace'] = trace_name
        
        if additional_pickle_data is not None:
            for key, value in additional_pickle_data.items():
                single_run_result[key] = value
        
        single_run_result.to_pickle(f'./results/{pickle_filename}')
        print(f"{Colors.bold}{Colors.yellow}Avg. Pen. {int(single_run_result['Average Penalty'].iloc[0])}{Colors.reset}")
        print(f"Policy. {single_run_result['Policy'].iloc[0]}")
        
        if dump_filename is not None:
            dump_files = [f for f in listdir('/tmp') if f.endswith('.dump')]
            assert len(dump_files) == 1
            move(f'/tmp/{dump_files[0]}', f'./results/{dump_filename}')
        
        
def run_full_ghost(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
    
    pickle_filename = f'FGHC-{trace_name}-extended-{cache_size}.pickle'
    run_test(fname, trace_name, cache_size, pickle_filename, 'full_ghost', 
                name='FGHC', additional_settings={**SETTINGS, **SIZE_SETTINGS})


def run_sampled(fname: str, trace_name: str, cache_size: int, round: int, seed: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    for sample_rate in range(1, 7):
        if (int(quantum_size) >> sample_rate > 0):
            SAMPLE_SETTINGS = {'sampled-hill-climber.sample-order-factor' : sample_rate, 
                            'sampled-hill-climber.adaption-multiplier' : 10}
            
            SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
            
            pickle_filename = f'sampled-O{sample_rate}-{trace_name}-extended-{cache_size}-R{round}.pickle'
            run_test(fname, trace_name, cache_size, pickle_filename, 'sampled_ghost', 
                    name=f'extended-O{sample_rate}', additional_settings={**SETTINGS, **SAMPLE_SETTINGS, **SIZE_SETTINGS, SEED_PATH: seed},
                    additional_pickle_data={'Round' : round, 'Seed': seed})


def run_random_hill_climber(fname: str, trace_name: str, cache_size: int, round: int, seed: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}

    pickle_filename = f'RHC-{trace_name}-extended-{cache_size}.pickle'
    run_test(fname, trace_name, cache_size, pickle_filename, 'random_climber', 
            name=f'RHC', additional_settings={**SETTINGS, **SIZE_SETTINGS, SEED_PATH: seed}, 
            additional_pickle_data={'Round' : round, 'Seed': seed})


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input', help="The input trace path", required=True)
    parser.add_argument('--rounds', help="number of round to perform", required=True, type=int)
    parser.add_argument('--round-index-start', help="The starting index for the round numbers", required=False, type=int, default=0)
    
    args = parser.parse_args()
    
    print(f'{Colors.pink}Running with args:\n{pprint.pformat(args)}{Colors.reset}')
        
    makedirs('./results', exist_ok=True)
    
    file = args.input

    print(f'{Colors.lightblue}Testing file: {file}{Colors.reset}')

    trace_name = get_trace_name(file)
    cache_size = SIZES.get(trace_name) * 10
    
    run_full_ghost(file, trace_name, cache_size)

    for round in tqdm.trange(args.rounds):
        seed = int.from_bytes(urandom(4), 'big')

        run_sampled(file, trace_name, cache_size, args.round_index_start + round + 1, seed)
        run_random_hill_climber(file, trace_name, cache_size, args.round_index_start + round + 1, seed)
    
    print(f'{Colors.bold}{Colors.green}Done\n#####################\n\n{Colors.reset}')

if __name__ == "__main__":
    main()
