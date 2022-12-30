import simulatools
import pprint
import re
from itertools import chain
from os import path, listdir, makedirs
import time
from datetime import timedelta


TRACES_DIR = '/home/nadav/caching/caffeine-main/simulator/src/main/resources/com/github/benmanes/caffeine/cache/simulator/parser/latency'


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


sizes = {'trace000' : 2 ** 15 , 'trace003' : 2 ** 9, 'trace004' : 2 ** 6, 'trace012' : 2 ** 10, 'trace024' : 2 ** 10, 'trace031' : 2 ** 16}


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
    suffix = re.findall('t_[0-9]*', fname)
    
    return suffix[0].lstrip('t_')
    

def main():
    files_tested = [f for f in listdir(TRACES_DIR)]
    
    for file in files_tested:
        with Timer():
            trace_name = get_trace_name(file)
            window_size = get_window_size(file)
            print(f'{Colors.bold}Processing file {file}{Colors.reset}')
            result_CA = simulatools.single_run('windowca', trace_file=file, trace_folder='latency', trace_format='LATENCY', 
                                                size=sizes[trace_name], additional_settings={'latency-estimation.strategy' : 'latest', 'ca-window.percent-main' : [0.1]},
                                                name=f'{trace_name}-CA') 
            
            # result = simulatools.single_run('wtlfu', trace_file=file, trace_folder='latency', trace_format='LATENCY', 
            #                                 size=sizes[trace_name], additional_settings={'window-tiny-lfu.percent-main' : [0.9]},
            #                                 name=f'{trace_name}-W{window_size}') #additional_settings={'latency-estimation.strategy' : 'latest'}
            
            
            print(f'{Colors.yellow}{trace_name} window {window_size} {Colors.purple}:\nResults with CA:{Colors.orange}{result_CA}{Colors.reset}') #Result without CA: {Colors.orange}{result}\n{Colors.purple}


if __name__ == "__main__":
    main()