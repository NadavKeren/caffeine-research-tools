from datetime import timedelta
import time

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
