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


SIZES = {'trace010' : 2 ** 10, 'trace024' : 2 ** 10, 'trace031' : 2 ** 16,
         'trace045' : 2 ** 12, 'trace034' : 2 ** 14, 'trace029' : 2 ** 9,
         'trace012' : 1024}

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
    

    
    for file in files_tested:
        trace_name = get_trace_name(file)
        print(f'{file}')
        
        cache_size = SIZES[trace_name]
        for lfu_percentage in LFU_PERCENTAGES:
            print(f'Running with {cache_size} and LFU: {int(lfu_percentage * 100)}%')
            single_run_result = simulatools.single_run('window_ca', trace_file=file, trace_folder='latency', 
                                                        trace_format='LATENCY', size=cache_size, 
                                                        additional_settings={**basic_settings, 
                                                                            'ca-window.percent-main' : [lfu_percentage]},
                                                        name=f'{file}-{cache_size}-WCA',
                                                        save = False)
            
            if (single_run_result is False):
                print(f'{Colors.bold}{Colors.red}Error in {file}: exiting{Colors.reset}')
            else:
                single_run_result['LFU Percentage'] = int(lfu_percentage * 100)
                single_run_result['Cache Size'] = cache_size
                single_run_result['Trace'] = trace_name
                single_run_result['File'] = file
                single_run_result.to_pickle(f'./results/{file}-{cache_size}-{int(lfu_percentage * 100)}-LFU-LRU-res.pickle')


if __name__ == "__main__":
    main()     