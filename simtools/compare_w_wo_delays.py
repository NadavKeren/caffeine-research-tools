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
TRACES_DIR = f'{caffeine_root}/simulator/src/main/resources/com/github/benmanes/caffeine/cache/simulator/parser/latency'


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


sizes = {'trace000' : 2 ** 13 , 'trace003' : 2 ** 6, 'trace004' : 2 ** 6, 'trace012' : 2 ** 10, 'trace024' : 2 ** 10, 'trace031' : 2 ** 16}


class Timer():
    def __init__(self, msg: str = None):
        self._start_time = None
        self._end_time = None
        self._msg = msg
    
    def __enter__(self):
        self._start_time = time.time()
        return self
    
    
    def __exit__(self, *exec_info):
        self._end_time = time.time()
        diff = self._end_time - self._start_time
        delta = timedelta(seconds=diff).total_seconds()
        minutes = int(delta) // 60
        seconds = delta - minutes * 60
        
        printed_str = None
        if self._msg is None:
            printed_str = f'{Colors.bold}{Colors.purple}Elapsed time: '
        else:
            printed_str = f'{Colors.bold}{Colors.purple}Elapsed time of {Colors.green}{self._msg}{Colors.purple}: '
        
        print(f'{printed_str}{Colors.cyan}{minutes}:{seconds:04.2f}{Colors.reset}')
        

def get_trace_name(fname: str):
    name = re.findall('Trace0[0-9][0-9]', fname)
    
    return name[0].lower()

def get_window_size(fname: str):
    suffix = re.findall('t_[0-9]*-[0-9]*', fname)
    if not suffix:
        suffix = re.findall('t_[0-9]*', fname)
    
    return suffix[0].lstrip('t_')


    

def main():
    files_tested = [f for f in listdir(TRACES_DIR) if (('Trace024' in f or 'Trace031' in f) and ('t_50' in f))]
    files_tested = sorted(files_tested, key=lambda fname: (get_trace_name(fname), get_window_size(fname)))
    pprint.pprint(files_tested)
    results_BB = list()
    results_BC = list()
    
    basic_settings = {'latency-estimation.strategy' : 'latest', 
                      'ca-bb-hill-climber-window.strategy' : ['simple'],
                      'ca-hill-climber-window.strategy' : ['simple'], 
                      'ca-bb-hill-climber-window.percent-main' : [0.99],
                      'ca-bb-hill-climber-window.cra.decayFactors' : [1],
                      'ca-bb-hill-climber-window.cra.max-lists' : [10]}
    #       cra {
            #   # The k parameter for benefit computation
            #   k = [1]
            #   # The reset factor - maximumSize * reset is the request count before adjusting the request count
            #   max-lists = [10]
            # }
    print(f'{Colors.bold}{Colors.yellow}Running from: {caffeine_root}\n########################################################\n {Colors.reset}')
    for file in files_tested:
        with Timer():
            print(f'{Colors.bold}Processing file {file}{Colors.reset}')
            trace_name = get_trace_name(file)
            window_sizes = get_window_size(file)
            print(window_sizes)
            penalty = window_sizes.split('-')[1]
            cache_size = sizes[trace_name]
            
            percentages = list(chain(arange(0.001, 0.01, 0.002), arange(0.1, 0.85, 0.15)))

            # for percentage in percentages:
            #     result = simulatools.single_run('window_ca_burst_block', trace_file=file, trace_folder='latency', 
            #                                     trace_format='LATENCY', size=cache_size, 
            #                                     additional_settings={**basic_settings,
            #                                                          'ca-bb-window.percent-burst-block' : percentage},
            #                                     name=f'{trace_name}-{penalty}-CA-BB-{percentage}') 
                
            #     if (result is False):
            #         print(f'{Colors.bold}{Colors.red}Error: exiting{Colors.reset}')
            #         exit(1)
                
            #     results_BB.append({'Type': 'WindowCAWithBB', 'Trace File' : trace_name, 'Penalty' : penalty, 
            #                        'Cache Size' : cache_size, 'Burst Window' : percentage, 'Hit Rate' : result[0],
            #                       'Average Latency' : result[1], 'Average Delayed Latency' : result[2]})
                
            #     print(f'{Colors.purple}Results with Burstiness-Block percentage {percentage}:{Colors.cyan}{result}{Colors.reset}')
            
            result = simulatools.single_run('adaptive_ca', trace_file=file, trace_folder='latency', trace_format='LATENCY', 
                                            size=cache_size, additional_settings=basic_settings,
                                            name=f'{trace_name}-{penalty}-adptive-CA')
            if (result is False):
                print(f'{Colors.bold}{Colors.red}Error: exiting{Colors.reset}')
                exit(1)

            results_BB.append({'Trace File' : trace_name, 'Penalty' : penalty, 'Cache Size' : cache_size, 
                               'Burst Window' : 0, 'Hit Rate' : result[0], 'Average Latency' : result[1]})
            print(f'{Colors.purple}Results with Adaptive CA:{Colors.cyan}{result}{Colors.reset}')
            
            for percentage in percentages:
                result = simulatools.single_run('adaptive_ca_burst', trace_file=file, trace_folder='latency', trace_format='LATENCY', 
                                                size = cache_size, additional_settings={**basic_settings,
                                                                                      'ca-bb-hill-climber-window.percent-burst-block' : percentage},
                                                name = f'{trace_name}-{penalty}-adptive-CA-BB-{percentage}')
                
                if (result is False):
                    print(f'{Colors.bold}{Colors.red}Error: exiting{Colors.reset}')
                    exit(1)
                
                results_BB.append({'Trace File' : trace_name, 'Penalty' : penalty, 'Cache Size' : cache_size, 
                                'Burst Window' : percentage, 'Hit Rate' : result[0], 'Average Latency' : result[1]})
                print(f'{Colors.purple}Results with Adaptive Burstiness-Block percentage {percentage}:{Colors.cyan}{result}{Colors.reset}')


    results_BB_df = pd.DataFrame.from_records(results_BB)
    # results_BC_df = pd.DataFrame.from_records(results_BC)
    results_BB_df.to_pickle('./results_adptive_burst_block_two_dist.pickle')
    # results_BC_df.to_pickle('./results_burst_cal.pickle')


if __name__ == "__main__":
    main()