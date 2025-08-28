import tqdm
import argparse

from os.path import *
from os import path, listdir, makedirs

from json import dump

DIR = './temp_processed'

def check_delayed_hits(fname: str, window_size: int) -> float:
    num_of_delayed_hits = 0
    with open(f'{DIR}/{fname}', 'r') as trace_file:
        requests = trace_file.readlines()
        num_of_items = len(requests)
        
        for idx in range(num_of_items):
            requests[idx] = requests[idx].strip('\n ')
        
        for idx in tqdm.trange(num_of_items, colour='magenta', leave=False):
            lower_range = max(idx - window_size, 0)
            for compared_idx in range(lower_range, idx):
                if (requests[idx] == requests[compared_idx]):
                    num_of_delayed_hits += 1
                    break
                    
    
    return (100.0 * num_of_delayed_hits) / num_of_items


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-w', '--window-size', help="the amount of future requests to check for delayed hits", type=int)
    
    args = parser.parse_args()
    
    traces_files = [f for f in listdir(DIR)]
    
    # percentages = list()
        
    # for i in tqdm.trange(len(traces_files), colour='cyan', leave=False):
    #     percentages.append(check_delayed_hits(traces_files[i], args.window_size))
            
    # result = sum(percentages) / len(percentages)
    
    result = check_delayed_hits(traces_files[1], args.window_size)
    print(f'{args.window_size}: {result}')
        
    
    
if __name__ == '__main__':
    main()