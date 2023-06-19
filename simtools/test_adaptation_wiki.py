import argparse
import simulatools
import pprint
import pickle
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
    

NUM_OF_QUANTA = [16, 32, 64]
ADAPTION_MULTIPLIERS = [1, 5, 10, 15]
CACHE_SIZE = 4096
TRACE_FORMAT = "WIKIPEDIA"
TRACE_FOLDER = "wiki"

def main():
    makedirs('./results', exist_ok=True)
    
    files = [f for f in listdir(f'{TRACES_DIR}/wiki')]
    
    print(f'{Colors.bold}{Colors.green}Testing the files:\n{Colors.reset}{pprint.pformat(files)}')
    
    for quanta_num in NUM_OF_QUANTA:
        for starting_LFU_quota in [1, quanta_num / 2, quanta_num - 1]:
            for adaption_multiplier in ADAPTION_MULTIPLIERS:
                settings = {"ghost-hill-climber-tiny-lfu.num-of-quanta" : quanta_num,
                            "ghost-hill-climber-tiny-lfu.initial-lfu-quota": starting_LFU_quota,
                            "ghost-hill-climber-tiny-lfu.adaption-multiplier": adaption_multiplier}
                
                print(f'{Colors.lightcyan}{Colors.bold}Running with: num of quanta: {quanta_num},' 
                      + f' lfu-start-quota: {starting_LFU_quota}, adaption-multiplier: {adaption_multiplier}{Colors.reset}')
                
                single_run_result = simulatools.single_run('naive_shadow', trace_files=files, trace_folder=TRACE_FOLDER, 
                                                           trace_format=TRACE_FORMAT, size=CACHE_SIZE, additional_settings=settings, 
                                                           save=False, 
                                                           name=f'wiki-{quanta_num}-{starting_LFU_quota}-{adaption_multiplier}-naive-shadow')
                
                if (single_run_result is False):
                    print(f'{Colors.bold}{Colors.red}Error in {trace_name}-{window_sizes}: exiting{Colors.reset}')
                    exit(1)
                else:
                    print(f'Results: {single_run_result["Hit Rate"]}')
                    single_run_result['Cache Size'] = CACHE_SIZE
                    single_run_result.to_pickle(f'./results/wiki-{quanta_num}-{starting_LFU_quota}-{adaption_multiplier}-naive-shadow.pickle')


if __name__ == "__main__":
    main()             