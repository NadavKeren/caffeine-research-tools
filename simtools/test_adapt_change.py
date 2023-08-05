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


SIZES = {'trace010' : 2 ** 10, 'trace024' : 2 ** 10, 'trace031' : 2 ** 16,
         'trace045' : 2 ** 12, 'trace034' : 2 ** 14, 'trace029' : 2 ** 9,
         'trace012' : 2 ** 10}


ADAPTIVE_PIPELINE_SETTINGS = {"adaptive-pipeline.num-of-block" : 3, "adaptive-pipeline.num-of-quanta" : 16, "adaptive-pipeline.adaption-multiplier" : 15,
                              "adaptive-pipeline.burst.aging-window-size" : 50, "adaptive-pipeline.burst.age-smoothing" : 0.0025, 
                              "adaptive-pipeline.burst.number-of-partitions" : 4}

ADAPTIVE_PIPELINE_LRU_START_SETTINGS = {**ADAPTIVE_PIPELINE_SETTINGS, "adaptive-pipeline.blocks.0.type": "LRU",
                                        "adaptive-pipeline.blocks.0.quota": 12, "adaptive-pipeline.blocks.0.decay-factor" : 1, 
                                        "adaptive-pipeline.blocks.0.max-lists" : 10,
                                        "adaptive-pipeline.blocks.1.type": "LFU",
                                        "adaptive-pipeline.blocks.1.quota": 2, "adaptive-pipeline.blocks.1.decay-factor" : 1, 
                                        "adaptive-pipeline.blocks.1.max-lists" : 10,
                                        "adaptive-pipeline.blocks.2.type": "BC",
                                        "adaptive-pipeline.blocks.2.quota": 2}

ADAPTIVE_PIPELINE_LFU_START_SETTINGS = {**ADAPTIVE_PIPELINE_SETTINGS, "adaptive-pipeline.blocks.0.type": "LRU",
                                        "adaptive-pipeline.blocks.0.quota": 2, "adaptive-pipeline.blocks.0.decay-factor" : 1, 
                                        "adaptive-pipeline.blocks.0.max-lists" : 10,
                                        "adaptive-pipeline.blocks.1.type": "LFU",
                                        "adaptive-pipeline.blocks.1.quota": 12, "adaptive-pipeline.blocks.1.decay-factor" : 1, 
                                        "adaptive-pipeline.blocks.1.max-lists" : 10,
                                        "adaptive-pipeline.blocks.2.type": "BC",
                                        "adaptive-pipeline.blocks.2.quota": 2}

ADAPTIVE_PIPELINE_BC_START_SETTINGS = {**ADAPTIVE_PIPELINE_SETTINGS, "adaptive-pipeline.blocks.0.type": "LRU",
                                        "adaptive-pipeline.blocks.0.quota": 2, "adaptive-pipeline.blocks.0.decay-factor" : 1, 
                                        "adaptive-pipeline.blocks.0.max-lists" : 10,
                                        "adaptive-pipeline.blocks.1.type": "LFU",
                                        "adaptive-pipeline.blocks.1.quota": 2, "adaptive-pipeline.blocks.1.decay-factor" : 1, 
                                        "adaptive-pipeline.blocks.1.max-lists" : 10,
                                        "adaptive-pipeline.blocks.2.type": "BC",
                                        "adaptive-pipeline.blocks.2.quota": 12}

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


def run_adaptive_pipeline(fname: str, trace_name: str, times: str, cache_size: int, ghost_size: int) -> None:
    quantum_size = cache_size // 16
    pickle_filename = f'adaptive-pipeline-{trace_name}-{times}-{cache_size}-{ghost_size}-LRU.pickle'
    dump_filename = f'adaptive-pipeline-adaptions-{trace_name}-{times}-{cache_size}-{ghost_size}-LRU.dump'
    
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'adaptive_pipeline', dump_filename, name="LRU",
             additional_settings={**ADAPTIVE_PIPELINE_LRU_START_SETTINGS, "adaptive-pipeline.quantum-size" : quantum_size,
                                  "adaptive-pipeline.ghost-size" : ghost_size})
    
    pickle_filename = f'adaptive-pipeline-{trace_name}-{times}-{cache_size}-{ghost_size}-LFU.pickle'
    dump_filename = f'adaptive-pipeline-adaptions-{trace_name}-{times}-{cache_size}-{ghost_size}-LFU.dump'
    
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'adaptive_pipeline', dump_filename, name="LFU",
             additional_settings={**ADAPTIVE_PIPELINE_LFU_START_SETTINGS, "adaptive-pipeline.quantum-size" : quantum_size,
                                  "adaptive-pipeline.ghost-size" : ghost_size})
    
    pickle_filename = f'adaptive-pipeline-{trace_name}-{times}-{cache_size}-{ghost_size}-BC.pickle'
    dump_filename = f'adaptive-pipeline-adaptions-{trace_name}-{times}-{cache_size}-{ghost_size}-BC.dump'
    
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'adaptive_pipeline', dump_filename, name="BC",
             additional_settings={**ADAPTIVE_PIPELINE_BC_START_SETTINGS, "adaptive-pipeline.quantum-size" : quantum_size, 
                                  "adaptive-pipeline.ghost-size" : ghost_size})
    
    
def run_test(fname: str, trace_name: str, times: str, cache_size: int, pickle_filename : str,
             algorithm : str, dump_filename : str = None, additional_settings = None, name = None, additional_pickle_data = None) -> None:
    now = datetime.datetime.now()
    print(f'{now.strftime("%H:%M:%S")}: {Colors.pink}Running {algorithm} on file: {fname} at pickle: {pickle_filename}{Colors.reset}')
    
    if (path.isfile(f'./results/{pickle_filename}')): # * Skipping tests with existing results        
        return
    
    settings = ADAPTIVE_PIPELINE_SETTINGS if additional_settings is None else {**ADAPTIVE_PIPELINE_SETTINGS, **additional_settings}
        
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
        
        if dump_filename is not None:
            dump_files = [f for f in listdir('/tmp') if f.endswith('.dump')]
            assert len(dump_files) == 1
            move(f'/tmp/{dump_files[0]}', f'./results/{dump_filename}')
        
        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--trace', help='The trace name to test, Default: ALL DEFINED', type=str,  required=False)
    
    args = parser.parse_args()
    
    print(f'{Colors.pink}Running with args:\n{pprint.pformat(args)}{Colors.reset}')
    files_tested = [f for f in listdir(f'{TRACES_DIR}/latency') if path.isfile(f'{TRACES_DIR}/latency/{f}')]
    
    if (args.trace is not None):
        files_tested = [f for f in files_tested if (args.trace in f.lower())]
        
    print(f'{Colors.bold}{Colors.yellow}Running from: {caffeine_root} for resources: {TRACES_DIR}\n########################################################\n {Colors.reset}')
    print(f'{Colors.bold}{Colors.cyan}Checking files:\n{pprint.pformat(files_tested, indent=4)}{Colors.reset}')
    
    makedirs('./results', exist_ok=True)
    
    ghost_sizes = [1] #, 2]
    for file in files_tested:
        print(f'{Colors.lightblue}Testing file: {file}{Colors.reset}')
        trace_name = get_trace_name(file)
        times = get_times(file)
        cache_size = SIZES.get(trace_name)
        
        for ghost_size in ghost_sizes:
            run_adaptive_pipeline(file, trace_name, times, cache_size, ghost_size)
                
    print(f'{Colors.bold}{Colors.green}Done\n#####################\n\n{Colors.reset}')


if __name__ == "__main__":
    main()     