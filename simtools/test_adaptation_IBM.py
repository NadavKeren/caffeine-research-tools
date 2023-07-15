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

CA_BB_SETTINGS = {"ca-bb-window.percent-main" : [0.9], "ca-bb-window.percent-main-protected" : 0.8,
                  "ca-bb-window.aging-window-size" : 300, "ca-bb-window.age-smoothing" : 0.01, 
                  "ca-bb-window.number-of-partitions" : 4, "ca-bb-window.burst-strategy" : "naive", 
                  "ca-bb-window.cra.decayFactors" : 1, "ca-bb-window.cra.max-lists" : 10}

CA_WINDOW_SETTINGS = {"ca-window.percent-main-protected" : 0.8, "ca-window.cra.decay-factor" : 1, "ca-window.cra.max-lists" : 10}

ADAPTIVE_CA_BB_SETTINGS = {"adaptive-ca-bb.num-of-quanta" : 32, "adaptive-ca-bb.quota-probation" : 4, "adaptive-ca-bb.quota-protected" : 16,
                           "adaptive-ca-bb.quota-window" : 4, "adaptive-ca-bb.quota-bc" : 8, "adaptive-ca-bb.adaption-multiplier" : 15}

ADAPTIVE_CA_SETTINGS = {"ca-hill-climber-window.strategy" : ["simple"], "ca-hill-climber-window.percent-main" : [0.875], "ca-hill-climber-window: percent-main-protected" : 0.85,
                        "ca-hill-climber-window.cra.decay-factor" : 1, "ca-hill-climber-window.cra.max-lists" : 10, 
                        "ca-hill-climber-window.simple.percent-pivot" : 0.0625, "ca-hill-climber-window.simple.percent-sample" : 10,
                        "ca-hill-climber-window.simple.tolerance" : 0, "ca-hill-climber-window.simple.step-decay-rate" : 0.98,
                        "ca-hill-climber-window.simple.sample-decay-rate" : 1, "ca-hill-climber-window.simple.restart-threshold" : 0.05}

SETTINGS = {**CA_BB_SETTINGS, **CA_WINDOW_SETTINGS, **ADAPTIVE_CA_BB_SETTINGS, **ADAPTIVE_CA_SETTINGS}


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


def run_adaptive_CA(fname: str, trace_name: str, times: str, cache_size: int) -> None:
    pickle_filename = f'adaptive-CA-{trace_name}-{times}.pickle'
    dump_filename = f'adaptive-CA-adaptions-{trace_name}-{times}.dump'
    
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'adaptive_ca', dump_filename)
    
    
def run_adaptive_CA_BB(fname: str, trace_name: str, times: str, cache_size: int) -> None:
    pickle_filename = f'adaptive-CA-BB-{trace_name}-{times}.pickle'
    dump_filename = f'adaptive-CA-BB-adaptions-{trace_name}-{times}.dump'
    
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'adaptive_ca_burst', dump_filename)
    
def run_static_CA_BB(fname: str, trace_name: str, times: str, cache_size: int) -> None:
    pickle_filename = f'static-CA-BB-{trace_name}-{times}-0.1-BC.pickle'
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'window_ca_burst_block', 
             additional_settings={"ca-bb-window.percent-burst-block" : 0.1}, name='BC-0.1', additional_pickle_data={'Burst Cache Percentage' : 10})
    
    pickle_filename = f'static-CA-BB-{trace_name}-{times}-0.8-BC.pickle'
    run_test(fname, trace_name, times, cache_size, pickle_filename, 'window_ca_burst_block', 
             additional_settings={"ca-bb-window.percent-burst-block" : 0.8}, name='BC-0.8', additional_pickle_data={'Burst Cache Percentage' : 80})
    
    
def run_window_CA(fname: str, trace_name: str, times: str, cache_size: int) -> None:
        pickle_filename = f'window-CA-{trace_name}-{times}-0.1-LRU.pickle'
        run_test(fname, trace_name, times, cache_size, pickle_filename, 'window_ca', 
                additional_settings={"ca-window.percent-main" : [0.9]}, name='LRU-0.1', additional_pickle_data={'LRU Percentage' : 10})
        
        pickle_filename = f'window-CA-{trace_name}-{times}-0.5-LRU.pickle'
        run_test(fname, trace_name, times, cache_size, pickle_filename, 'window_ca', 
                additional_settings={"ca-window.percent-main" : [0.5]}, name='LRU-0.5', additional_pickle_data={'LRU Percentage' : 50})
        
        pickle_filename = f'window-CA-{trace_name}-{times}-0.9-LRU.pickle'
        run_test(fname, trace_name, times, cache_size, pickle_filename, 'window_ca', 
                additional_settings={"ca-window.percent-main" : [0.1]}, name='LRU-0.9', additional_pickle_data={'LRU Percentage' : 90})

        
def run_test(fname: str, trace_name: str, times: str, cache_size: int, pickle_filename : str,
             algorithm : str, dump_filename : str = None, additional_settings = None, name = None, additional_pickle_data = None) -> None:
    print(f'{Colors.pink}Running {algorithm} on file: {fname} at pickle: {pickle_filename}{Colors.reset}')
    
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
        
        if dump_filename is not None:
            dump_files = [f for f in listdir('/tmp') if f.endswith('.dump')]
            assert len(dump_files) == 1
            move(f'/tmp/{dump_files[0]}', f'./results/{dump_filename}')
        
        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--trace', help='The trace name to test, Default: ALL DEFINED', type=str,  required=False)
    
    args = parser.parse_args()
    
    print(f'{Colors.pink}Running with args:\n{pprint.pformat(args)}{Colors.reset}')
    files_tested = [f for f in listdir(f'{TRACES_DIR}/latency')]
    
    if (args.trace is not None):
        files_tested = [f for f in files_tested if (args.trace in f.lower())]
        
    print(f'{Colors.bold}{Colors.yellow}Running from: {caffeine_root} for resources: {TRACES_DIR}\n########################################################\n {Colors.reset}')
    print(f'{Colors.bold}{Colors.cyan}Checking files:\n{pprint.pformat(files_tested, indent=4)}{Colors.reset}')
    
    makedirs('./results', exist_ok=True)
    
    for file in files_tested:
        print(f'{Colors.lightblue}Testing file: {file}{Colors.reset}')
        trace_name = get_trace_name(file)
        times = get_times(file)
        cache_size = SIZES.get(trace_name)
        
        if cache_size is not None:
            run_adaptive_CA(file, trace_name, times, cache_size)
            run_adaptive_CA_BB(file, trace_name, times, cache_size)
            run_static_CA_BB(file, trace_name, times, cache_size)
            run_window_CA(file, trace_name, times, cache_size)
                
    print(f'{Colors.bold}{Colors.green}Done\n#####################\n\n{Colors.reset}')

if __name__ == "__main__":
    main()     
