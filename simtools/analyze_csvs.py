import csv
import re
from os import listdir
# import glob
from typing import Dict
import matplotlib.pyplot as plt

# reader = csv.DictReader(csvfile)
# results = { line['Policy'] : (f"{float(line['Hit Rate']):.2f}%", float(line['Average Penalty']), float(line['Average Penalty not including delayed hits'])) for line in reader }

blue_color='#332288'
green_color='#117733'
yellow_color='#DDCC77'
red_color='#CC6677'
pink_color='#AA4499'

CSVS_DIR = './csvs/'

def get_trace_name(fname: str) -> str:
    name = re.findall('Trace0[0-9][0-9]', fname)
    
    return name[0].lower()

def get_window_size(fname: str) -> int:
    suffix = re.findall('t_[0-9]*', fname)
    
    return int(suffix[0].lstrip('t_'))

def is_cost_aware(policy_name : str) -> bool:
    res = re.search("sketch.WindowCATinyLfu", policy_name)

    return not res is None

def get_simple_policy_name(policy_name : str) -> str:
    if (is_cost_aware(policy_name)):
        return "W-LFU CA"
    return None

def process_csvs() -> Dict:
    csvs = [f for f in listdir(CSVS_DIR)]
    
    results = dict()
    
    for file in csvs:
        trace = get_trace_name(file)
        latency = get_window_size(file)
        with open(f'{CSVS_DIR}/{file}', 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            curr_results = [(line['Policy'], float(line['Hit Rate']), float(line['Average Penalty']), 
                                               float(line['Average Penalty not including delayed hits'])) for line in reader]
            
            simple_policy_name = get_simple_policy_name(curr_results[0][0])
            hit_rate = curr_results[0][1]
            average_pen = curr_results[0][2]
            average_pen_wo_delayed = curr_results[0][3]
            prev_trace_result = results.get(trace, dict())
            prev_trace_result[latency] = {'Hit Rate' : hit_rate, 'Average Penalty' : average_pen, 'Average without Delayed' : average_pen_wo_delayed}
            
            results[trace] = prev_trace_result
    
    return results


def analyze_single_trace(trace_results : Dict):
    trace_name = trace_results[0]
    latencys = []
    hit_ratio = []
    avg_pen = []
    avg_pen_wo_delayed = []
    
    keys = list(trace_results[1].keys())
    keys.sort()
    sorted_results = {k : trace_results[1][k] for k in keys}
    for item in sorted_results.items():
        latencys.append(item[0])
        hit_ratio.append(item[1]['Hit Rate'])
        avg_pen.append(item[1]['Average Penalty'])
        avg_pen_wo_delayed.append(item[1]['Average without Delayed'])
    
    plt.yscale('log')
    plt.xscale('log')
    plt.plot(latencys, avg_pen, marker = 'o', color = blue_color, linestyle = ':', label = 'Average penalty including delayed hits')
    plt.plot(latencys, avg_pen_wo_delayed, marker = '^' , color = red_color, linestyle = ':', label = 'Average penalty without delayed hits')
    plt.legend(loc='upper left')
    plt.title(f"Comparison of average time as a function of latency for {trace_name}")
    plt.xlabel('Latency (ms, log scale)')
    plt.ylabel('Average Penalty (ms, log scale)')
    plt.savefig(f'./{trace_name}_results.svg', dpi=300)
    plt.close()
    
    plt.xscale('log')
    plt.plot(latencys, hit_ratio, marker = 'x', color = green_color, linestyle = ':')
    plt.title(f"Hit Rate as a function of latency for {trace_name}")
    plt.xlabel('Latency (ms, log scale)')
    plt.ylabel('Hit Rate (%)')
    plt.savefig(f'./{trace_name}_hit_rate.svg', dpi=300)
    plt.close()
    
    # print(latencys)
    # print(avg_pen)
    # print(avg_pen_wo_delayed)

def main():
    results = process_csvs()
    for trace_result in results.items():
        analyze_single_trace(trace_result)

    # print (results)
   
    
if __name__ == '__main__':
    main()