import argparse
import simulatools
import pprint
import pickle
import re
from itertools import chain, product
from os import path, listdir, makedirs
from shutil import move
import time
from datetime import timedelta
import datetime
import json

import pandas as pd
from numpy import arange

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


def get_times(fname: str):
    temp_fname = fname.lower()
    times = re.findall('(?<=trace0[0-9][0-9]-)[a-zA-Z0-9-]*', temp_fname)
    
    return times[0]


def run_test(fname: str, trace_name: str, times: str, cache_size: int, pickle_filename : str,
             algorithm : str, dump_filename : str = None, additional_settings = None, name = None, additional_pickle_data = None) -> None:
    now = datetime.datetime.now()
    print(f'{now.strftime("%H:%M:%S")}: {Colors.pink}Running {algorithm} on trace: {trace_name}, size: {cache_size}{Colors.reset}' + f' Name: {name}' if name is not None else "")
    
    if (path.isfile(f'./results/{pickle_filename}')): # * Skipping tests with existing results        
        return
    
    settings = SETTINGS if additional_settings is None else {**SETTINGS, **additional_settings}
        
    single_run_result = simulatools.single_run(algorithm, trace_files=[fname], trace_folder='latency', 
                                                trace_format='LATENCY', size=cache_size,
                                                additional_settings=settings,
                                                name=f'{algorithm}-{trace_name}-{times}' if name is None else f'{algorithm}-{trace_name}-{times}-{name}',
                                                save = False, verbose = False)
    
    if (single_run_result is False):
        print(f'{Colors.bold}{Colors.red}Error in {fname}: exiting{Colors.reset}')
        exit(1)
    else:                    
        single_run_result['Cache Size'] = cache_size
        single_run_result['Trace'] = trace_name
        single_run_result['Times'] = times
        
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
        
        
def run_full_ghost(fname: str, trace_name: str, times: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
    
    pickle_filename = f'FGHC-{trace_name}-extended-{times}-{cache_size}.pickle'
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'full_ghost', 
                name='FGHC', additional_settings={**SETTINGS, **SIZE_SETTINGS})


def run_sampled(fname: str, trace_name: str, times: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    for sample_rate in range(1, 11):
        if (int(quantum_size) >> sample_rate > 0):
            SAMPLE_SETTINGS = {'sampled-hill-climber.sample-order-factor' : sample_rate, 
                            'sampled-hill-climber.adaption-multiplier' : 10}
            
            SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
            
            pickle_filename = f'sampled-O{sample_rate}-{trace_name}-extended-{times}-{cache_size}.pickle'
            run_test(fname, trace_name, times, cache_size, pickle_filename, 'sampled_ghost', 
                    name=f'extended-O{sample_rate}', additional_settings={**SETTINGS, **SAMPLE_SETTINGS, **SIZE_SETTINGS},
                    additional_pickle_data={'Sample Rate' : 1 / sample_rate})


def run_additional(fname: str, trace_name: str, times: str, cache_size: int) -> None:
    # pickle_filename = f'CA-ARC-{trace_name}-{times}-{cache_size}.pickle'
    # run_test(fname, trace_name, times, cache_size, pickle_filename, 'ca_arc')
    
    pickle_filename = f'Hyperbolic-{trace_name}-{times}-{cache_size}.pickle'
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'hyperbolic')
    
    pickle_filename = f'GDWheel-{trace_name}-{times}-{cache_size}.pickle'
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'gdwheel')
    
    pickle_filename = f'YanLi-{trace_name}-{times}-{cache_size}.pickle'
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'yan_li')
        
        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input', help="The input trace path", required=True)
    # parser.add_argument('--trace', help='The trace name to test, Default: ALL DEFINED', type=str,  required=False)
    # parser.add_argument('--additionals', help='Run of the additional algorithms for the comparison', action='store_true', required=False)
    
    args = parser.parse_args()
    
    print(f'{Colors.pink}Running with args:\n{pprint.pformat(args)}{Colors.reset}')
        
    makedirs('./results', exist_ok=True)
    
    file = args.input

    print(f'{Colors.lightblue}Testing file: {file}{Colors.reset}')

    trace_name = get_trace_name(file)
    times = get_times(file)
    optimal_size = SIZES.get(trace_name) * 10
    cache_sizes = [optimal_size // 4, optimal_size // 2, optimal_size, optimal_size * 2, optimal_size * 4]
    
    for cache_size in cache_sizes:
        run_full_ghost(file, trace_name, times, cache_size)
        run_sampled(file, trace_name, times, cache_size)
        
    print(f'{Colors.bold}{Colors.green}Done\n#####################\n\n{Colors.reset}')

if __name__ == "__main__":
    main()