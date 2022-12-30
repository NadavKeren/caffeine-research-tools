import simulatools
import pprint
from itertools import chain

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

def main():
    # num_of_caches = len(simulatools.Trace.latency_with_factors[1].typical_caches())
    # for i in range(num_of_caches):
    #     retcode = simulatools.single_run('windowca', 'l_normal_single_32', size=i+1, verbose=True)
    #     if retcode is False:
    #         return 1
    
    # cache_size = 2 ** 14
    # non_CA_results = { f'factor {i}' : 
    #                     simulatools.single_run('wtlfu', trace_file=f'diff_factor_{i}.0.trace.xz', trace_folder='latency', 
    #                                    trace_format='LATENCY', size=cache_size)
    #                     for i in range(2,10)
    #                  }
    
    # factors = [20] #list(range(2,10))
    # factors = list(chain(range(10, 100, 10), range(100, 500, 50), range(500, 2000, 100), range(2000, 10000, 1000)))
    cache_size = 2 ** 14
    factors = [10, 50, 100, 250, 500, 1000, 1500]
    factor = 100
    cache_sizes = [2 ** i for i in range(9, 16)]
    # print(f'{Colors.bold}{Colors.cyan}############################### Latest Latency CA: ###############################{Colors.reset}\n')
    
    # latest_latency_results = { f'factor {i}' :
    #                             simulatools.single_run('windowca', trace_file=f'IBM_diff_factor_{i}.xz', trace_folder='latency', 
    #                                                    trace_format='LATENCY', additional_settings={'latency-estimation.strategy' : 'latest'}, 
    #                                                    size=cache_size, name="latest-simple")
    #                            for i in factors
    #                          }
    # print(f'{Colors.bold}{Colors.yellow}############################### Variable Factor: ###############################{Colors.reset}\n')
    # pprint.pprint(latest_latency_results)
    
    # latest_latency_results = { f'cache size {size}' : simulatools.single_run('windowca', trace_file=f'IBM_diff_factor_{factor}.xz', trace_folder='latency', 
    #                                                    trace_format='LATENCY', additional_settings={'latency-estimation.strategy' : 'latest'}, 
    #                                                    size=size, name="latest-simple")
    #                           for size in cache_sizes }
    # print(f'{Colors.bold}{Colors.yellow}############################### Variable Size: ###############################{Colors.reset}\n')
    # pprint.pprint(latest_latency_results)
    
    # print(f'{Colors.bold}{Colors.cyan}############################### Latest including delayed Latency CA: ###############################{Colors.reset}\n')
    # latest_latency_results = { f'factor {i}' :
    #                             simulatools.single_run('windowca', trace_file=f'IBM_diff_factor_{i}.xz', trace_folder='latency', 
    #                                                    trace_format='LATENCY', additional_settings={'latency-estimation.strategy' : 'latest-with-delayed-hits'}, 
    #                                                    size=2 ** 14, name="latest-with-delays")
    #                            for i in factors
    #                          }
    # print(f'{Colors.bold}{Colors.yellow}############################### Variable Factor: ###############################{Colors.reset}\n')
    # pprint.pprint(latest_latency_results)
    
    # latest_latency_results = { f'cache size {size}' : simulatools.single_run('windowca', trace_file=f'IBM_diff_factor_{factor}.xz', trace_folder='latency', 
    #                                                    trace_format='LATENCY', additional_settings={'latency-estimation.strategy' : 'latest-with-delayed-hits'}, 
    #                                                    size=size, name="latest-with-delays")
    #                           for size in cache_sizes }
    # print(f'{Colors.bold}{Colors.yellow}############################### Variable Size: ###############################{Colors.reset}\n')
    # pprint.pprint(latest_latency_results)
    
    
    window_sizes=[0.99, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]
    print(f'{Colors.bold}{Colors.cyan}############################### Testing window sizes: ###############################{Colors.reset}\n')
    latest_latency_results = { f'Window size {window_size}' : simulatools.single_run('windowca', trace_file=f'IBM_diff_factor_{factor}.xz', trace_folder='latency', 
                                                    trace_format='LATENCY', additional_settings={'latency-estimation.strategy' : 'latest', 'ca-window.percent-main' : [window_size]}, 
                                                    size=cache_size, name=f"latest-{window_size}")
                                for window_size in window_sizes}
    
    print(f'{Colors.bold}{Colors.yellow}############################### Latest time only: ###############################{Colors.reset}\n')
    pprint.pprint(latest_latency_results)
    latest_latency_results = { f'Window size {window_size}' : simulatools.single_run('windowca', trace_file=f'IBM_diff_factor_{factor}.xz', trace_folder='latency', 
                                                trace_format='LATENCY', additional_settings={'latency-estimation.strategy' : 'latest-with-delayed-hits', 'ca-window.percent-main' : [window_size]}, 
                                                size=cache_size, name=f"latest-{window_size}")
                            for window_size in window_sizes}
    
    print(f'{Colors.bold}{Colors.yellow}############################### Including delayed hits estimation: ###############################{Colors.reset}\n')
    pprint.pprint(latest_latency_results)
    
    # true_average_results = { f'factor {i}' :
    #                          simulatools.single_run('windowca', trace_file=f'diff_factor_{i}.xz', trace_folder='latency', 
    #                                                 trace_format='LATENCY_ORACLE', additional_settings={'latency-estimation.strategy' : 'latest'},
    #                                                 size=cache_size)
    #                          for i in factors
    #                         }
    
    # results = { simulatools.Trace.multi1.typical_caches()[i-1] :
    #             simulatools.single_run('lru', 'multi3', i)
    #             for i in range(1,1+8)
    #           }
    
    # print('############################### Without CA: ###############################\n')
    # pprint.pprint(non_CA_results)
    
    # print('############################### True Average CA: ###############################\n')
    # pprint.pprint(true_average_results)

if __name__ == "__main__":
    main()

