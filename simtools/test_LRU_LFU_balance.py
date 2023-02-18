import argparse
import simulatools
import pprint
import re
from itertools import chain
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


SIZES = [2 ** 10, 2 ** 14, 2 ** 18]
LFU_PERCENTAGES = arange(0.1, 1.0, 0.1)


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

def get_window_size(fname: str):
    suffix = re.findall('_[0-9]*_[0-9]*', fname)
    if not suffix:
        suffix = re.findall('_[0-9]*', fname)
    
    return suffix[0].lstrip('_')


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--trace', help='The trace name to test', type=str,  required=True)
    parser.add_argument('--time', help='The time of the second dist to check, Default: ALL', 
                        type=int, required=False)
    
    args = parser.parse_args()
    
    print(f'{Colors.pink}Running with args:\n{pprint.pformat(args)}{Colors.reset}')
    if (args.time is None):
        files_tested = [f for f in listdir(f'{TRACES_DIR}/latency') if (args.trace in f.lower())]
    else:
        files_tested = [f for f in listdir(f'{TRACES_DIR}/latency') if (args.trace in f.lower() 
                                                           and str(args.time) in f)]
        
    print(f'{Colors.bold}{Colors.yellow}Running from: {caffeine_root} for resources: {TRACES_DIR}\n########################################################\n {Colors.reset}')
    print(f'{Colors.bold}{Colors.cyan}Checking files:\n{pprint.pformat(files_tested, indent=4)}{Colors.reset}')
    
    
    basic_settings = {'latency-estimation.strategy' : 'latest',
                      'ca-hill-climber-window.strategy' : ['simple']}

    results = pd.DataFrame()
    
    makedirs('./results', exist_ok=True)
    
    for file in files_tested:
        trace_name = get_trace_name(file)
        window_sizes = get_window_size(file)
        print(f'Trace: {trace_name}, Window sizes: {window_sizes}')
        
        for cache_size in SIZES:
            for lfu_percentage in LFU_PERCENTAGES:
                print(f'Running with {cache_size} and LRU: {lfu_percentage * 100}\%')
                single_run_result = simulatools.single_run('window_ca', trace_file=file, trace_folder='latency', 
                                                           trace_format='LATENCY', size=cache_size, 
                                                           additional_settings={**basic_settings, 
                                                                                'ca-window.percent-main' : [lfu_percentage]},
                                                           name=f'{trace_name}-{window_sizes}-{cache_size}-WCA',
                                                           save = False)
                
                if (single_run_result is False):
                    print(f'{Colors.bold}{Colors.red}Error in {trace_name}-{window_sizes}: exiting{Colors.reset}')
                else:
                    latency = window_sizes.split('_')[1]
                    single_run_result['LFU Percentage'] = lfu_percentage
                    single_run_result['Cache Size'] = cache_size
                    single_run_result['Latency'] = latency
                    single_run_result['Trace'] = trace_name
                    results = pd.concat([results, single_run_result], ignore_index=True)
                    results.to_pickle(f'./results/{trace_name}-{window_sizes}-{cache_size}-LFU-LRU-res.pickle')


if __name__ == "__main__":
    main()     