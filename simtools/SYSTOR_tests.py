import argparse
import simulatools
import pprint
import pickle
import re
from itertools import chain, product
from os import path, listdir, makedirs
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


SIZES = {'trace010' : 2 ** 10, 'trace024' : 2 ** 9, 'trace031' : 2 ** 16,
         'trace045' : 2 ** 12, 'trace034' : 2 ** 14, 'trace029' : 2 ** 9,
         'trace012' : 2 ** 10}
BB_PERCENTAGES = arange(0.1, 1, 0.1)

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


def run_baseline(fname: str, trace_name: str, cache_size: int, optimal_LFU_percentage: float):
    pickle_filename = f'{fname}-{cache_size}-{int(optimal_LFU_percentage * 100)}-Baseline-SYSTOR.pickle'
    
    if (not path.isfile(f'./results/{pickle_filename}')): # * Skipping baseline if exists
        print(f'{Colors.cyan}Running baseline for {fname} with size {cache_size}{Colors.reset}\n')
        single_run_result = simulatools.single_run('window_ca', trace_files=[fname], trace_folder='latency', 
                                                   trace_format='LATENCY', size=cache_size, 
                                                   additional_settings={'ca-window.percent-main' : [optimal_LFU_percentage]},
                                                   name=f'{fname}-{cache_size}-CA-Baseline',
                                                   save = False)
        
        if (single_run_result is False):
            print(f'{Colors.bold}{Colors.red}Error in {fname}: exiting{Colors.reset}')
            exit(1)
        else:       
            single_run_result['BB Percentage'] = 0
            single_run_result['Cache Size'] = cache_size
            single_run_result['Trace'] = trace_name
            single_run_result['Aging Alpha'] = 0
            single_run_result['Aging Mechanism'] = 'none'
            
            single_run_result.to_pickle(f'./results/{pickle_filename}')   


def run_single_conf(fname, basic_settings, trace_name, optimal_LFU_percentage, 
                    cache_size, bb_percentage):
    pickle_filename = f'{fname}-{cache_size}-{bb_percentage}-{int(optimal_LFU_percentage * 100)}-SYSTOR.pickle'
    if (not path.isfile(f'./results/{pickle_filename}')): # * Skipping tests with existing results        
        if (optimal_LFU_percentage < 0 or optimal_LFU_percentage > 1):
            print(f'{Colors.bold}{Colors.red}Error in {fname}: Bad optimal, exiting{Colors.reset}')
            exit(1)
        
        current_run_settings = {'ca-bb-window.percent-main' : [optimal_LFU_percentage],
                                'ca-bb-window.percent-burst-block' : bb_percentage,
                                "ca-bb-window.aging-window-size" : 50, 
                                "ca-bb-window.age-smoothing" : 0.0025}
        settings = {**basic_settings, 
                    **current_run_settings}
        
        print(f'{pprint.pformat(current_run_settings)}\n')
        
        single_run_result = simulatools.single_run('window_ca_burst_block', trace_files=[fname], trace_folder='latency', 
                                                    trace_format='LATENCY', size=cache_size, 
                                                    additional_settings=settings,
                                                    name=f'{fname}-{cache_size}-WBBCA',
                                                    save = False)
        
        if (single_run_result is False):
            print(f'{Colors.bold}{Colors.red}Error in {fname}: exiting{Colors.reset}')
            exit(1)
        else:
            single_run_result['Point Of Comparison'] = optimal_LFU_percentage
            single_run_result['BB Percentage'] = int(bb_percentage * 100)
            single_run_result['Cache Size'] = cache_size
            single_run_result['Trace'] = trace_name
            single_run_result['Aging Window Size'] = 50
            single_run_result['Aging Alpha'] = 0.0025
            
            single_run_result.to_pickle(f'./results/{pickle_filename}')

def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--trace', help='The trace name to test, Default: ALL DEFINED', type=str,  required=False)
    parser.add_argument('--optimals', help='The path to a pickle file containing the optimal results for each trace for reference to the correct LFU-LRU ratio to use', 
                        type=str, required=True)
    parser.add_argument('--time', help='The time of the second dist to check, Default: ALL', 
                        type=int, required=False)
    parser.add_argument('--random-aging', help='Toggle for the naive random aging estimation for baseline of the aging mechanism', action='store_true')
    
    args = parser.parse_args()
    
    print(f'{Colors.pink}Running with args:\n{pprint.pformat(args)}{Colors.reset}')
    files_tested = [f for f in listdir(f'{TRACES_DIR}/latency')]
    
    if (args.time is not None):
        files_tested = [f for f in files_tested if (str(args.time) in f)]
        
    if (args.trace is not None):
        files_tested = [f for f in files_tested if (args.trace in f.lower())]
        
    print(f'{Colors.bold}{Colors.yellow}Running from: {caffeine_root} for resources: {TRACES_DIR}\n########################################################\n {Colors.reset}')
    print(f'{Colors.bold}{Colors.cyan}Checking files:\n{pprint.pformat(files_tested, indent=4)}{Colors.reset}')
    
    if (args.random_aging is not True):
        basic_settings = {'latency-estimation.strategy' : 'latest',
                          'ca-bb-window.burst-strategy' : 'moving-average',
                          'ca-bb-window.number-of-partitions' : 4}
    else:
        basic_settings = {'latency-estimation.strategy' : 'latest',
                          'ca-bb-window.burst-strategy' : 'random'}
    
    makedirs('./results', exist_ok=True)
    
    with open(f'{args.optimals}', 'rb') as optimals_file:
        optimals = pickle.load(optimals_file)
    
    for file in files_tested:
        trace_name = get_trace_name(file)
        print(f'{file}')
        cache_size = SIZES.get(trace_name)
        
        if cache_size is not None:
            optimal_LFU_percentage = optimals.get((trace_name, args.time, cache_size))[0] / 100
            
            run_baseline(file, trace_name, cache_size, optimal_LFU_percentage)
            run_baseline(file, trace_name, cache_size, 0.5)
            for bb_percentage in BB_PERCENTAGES:
                run_single_conf(file, basic_settings, trace_name, 
                                optimal_LFU_percentage, cache_size, bb_percentage)
                run_single_conf(file, basic_settings, trace_name, 0.5, cache_size, bb_percentage)
                
    print(f'{Colors.bold}{Colors.green}Done\n#####################\n\n{Colors.reset}')

if __name__ == "__main__":
    main()     
