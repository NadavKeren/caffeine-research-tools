import tqdm
import pickle
import matplotlib.pyplot as plt

import argparse

from os.path import *
from os import path, listdir, makedirs

from itertools import islice

from typing import List
from enum import Enum
from dataclasses import dataclass

from math import inf


DIR = "/home/nadav/Thesis/caching/traces" # TODO: complete this
LATENCY_BINS = (0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2, 2.5, 3, 3.5, 4, 4.5, 5, 10, 15, 20, inf)
DESPLAID_BINS = (0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2, 2.5, 3, 3.5, 4, 4.5, 5, 10, 15, 20, 50)

class DelayedCases(Enum):
    START_ALIGNED = 0 #* Case 1
    UNIFORMLY_DIST = 1 #* Case 2 and Case 6
    END_ALIGNED = 2 #* Case 3
    SINGLE_REQUEST = 3 #* No delayed hits - Cases 4 and 5
    
    def __str__(self):
        return self.name
    
    @classmethod
    def from_dist(cls, dist: List[int]):
        assert len(dist) == 3
        
        total = sum(dist)
        if (total == 0):
            return DelayedCases.SINGLE_REQUEST
        
        first_third_percentage = dist[0] / total
        last_third_percentage = dist[2] / total
        
        if (first_third_percentage >= 0.7):
            return DelayedCases.START_ALIGNED
        
        if (last_third_percentage >= 0.7):
            return DelayedCases.END_ALIGNED
        
        return DelayedCases.UNIFORMLY_DIST


@dataclass # Add (slots = True) when conda supports 3.10
class AggregatedLatency():
    """ A data class for storing the aggregated latency of an entry """
    agg_latency : int = 0
    num_of_delayed_hits : int = 0


class Entry():
    __slots__ = ('timestamp', 'object_id')
    def __init__(self, timestamp:int, object_id: str):
        self.timestamp = timestamp
        self.object_id = object_id
        
    def __str__(self):
        return f'{self.timestamp} : {self.object_id}'
    

def parse_batch_from_file(fname: str, start_line: int, batch_size: int) -> List[Entry]:
    with open(f'{DIR}/{fname}', 'r') as trace_file:
        lines = [line for line in islice(trace_file, start_line, start_line + batch_size)]
    
    splitted = [line.strip(' \n').split(' ') for line in lines]
    requests = [Entry(int(line[0]), line[2]) for line in splitted]
    
    return requests


def check_delayed_hits(fname: str, window_size: int, batch_size: int) -> float:
    REREAD_OVERLAP_SIZE = int(batch_size / 10)
    THIRD_WINDOW_SIZE = window_size / 3
    delayed_hits_dist = [0 for i in range(window_size * 10)]
    delayed_latency_dist = [0 for _ in LATENCY_BINS]
    items_dict = dict()
    
    requests: List[Entry] = parse_batch_from_file(fname, 0, batch_size)
    next_batch_start = batch_size - REREAD_OVERLAP_SIZE # this allows some overlapping so we could test the new items with previous items read.
    
    while (requests):    
        for idx in range(min(len(requests), batch_size - REREAD_OVERLAP_SIZE)):
            curr_timestamp = requests[idx].timestamp
            lower_third_mark = curr_timestamp + THIRD_WINDOW_SIZE
            upper_range = curr_timestamp + window_size
            upper_third_mark = upper_range - THIRD_WINDOW_SIZE
            curr_obj_id = requests[idx].object_id
            
            curr_agg_latency = 0
            
            j = idx + 1
            num_of_delayed_hits = 0
            
            while (j < len(requests) and requests[j].timestamp <= upper_range):
                if (requests[j].object_id == curr_obj_id):
                    num_of_delayed_hits += 1
                    curr_agg_latency += (upper_range - requests[j].timestamp)
                j += 1
                
            curr_agg_latency /= window_size
            
            for bin, max_bin_val in enumerate(LATENCY_BINS):
                if (curr_agg_latency <= max_bin_val):
                    delayed_latency_dist[bin] += 1
                    break
            
            delayed_hits_dist[num_of_delayed_hits] += 1
            curr_item_entry : AggregatedLatency = items_dict.get(curr_obj_id, AggregatedLatency())
            curr_item_entry.agg_latency += curr_agg_latency
            curr_item_entry.num_of_delayed_hits += 1
            items_dict[curr_obj_id] = curr_item_entry
            
        requests: List[Entry] = parse_batch_from_file(fname, next_batch_start, batch_size)
        next_batch_start += batch_size - REREAD_OVERLAP_SIZE

    num_of_items = sum(1 for line in open(f'{DIR}/{fname}', 'r'))
    percentages = [100.0 * num / num_of_items for num in delayed_hits_dist]
    delayed_latency_dist = [100.0 * num / num_of_items for num in delayed_latency_dist]
                
    return percentages, delayed_latency_dist, items_dict


def addlabels(x, y, height:int):
    for i in range(len(x)):
        plt.text(i, y[i] + height, f'{y[i]:.2f}', ha = 'center')


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-w', '--window-size', help="the timeframe (in miliseconds) to check for delayed hits", type=int)
    parser.add_argument('-b', '--batch-size', help="the batch-size to read each time, should be at least 50 times bigger than the window size", type=int)
    parser.add_argument('-d', '--dir', help="The trace directory within the origin dir")
    args = parser.parse_args()
    args.dir = args.dir.strip(" ")
    traces_files = [f for f in listdir(f'{DIR}/{args.dir}') if f.startswith('IBMObjectStoreTrace')]
    
    # percentages = list()
        
    # for i in tqdm.trange(len(traces_files), colour='cyan', leave=False):
    #     percentages.append(check_delayed_hits(traces_files[i], args.window_size))
            
    # result = sum(percentages) / len(percentages)
    
    dist, delayed_latency_dist, items_dict = check_delayed_hits(f'{args.dir}/{traces_files[0]}', args.window_size, args.batch_size)
    bins = ["0", "1", "2", "3", "4 - 5", "6 - 10", "11 - 20", "20 - 50", "> 50"]
    agg = [0] * len(bins)
    agg[0] = dist[0]
    agg[1] = dist[1]
    agg[2] = dist[2]
    agg[3] = dist[3]
    agg[4] = sum(dist[4:5])
    agg[5] = sum(dist[6:10])
    agg[6] = sum(dist[11:20])
    agg[7] = sum(dist[21:50])
    agg[8] = sum(dist[51:])
    
    plt.bar(bins, agg)
    plt.grid(True)
    plt.title(f'Distribution of number of delayed hits in windows of size {args.window_size}')
    plt.ylim(0, 100)
    addlabels(bins, agg, 5)
    plt.savefig(f'{DIR}/{args.dir}/dist_{args.window_size}.png', dpi=300)
    # print(f'{args.window_size}: {result}')
    plt.close()
    
    plt.bar(DESPLAID_BINS, delayed_latency_dist)
    plt.grid(True)
    plt.title(f'Distribution of delayed hits latencies in windows of size {args.window_size}')
    plt.ylim(0, 100)
    # plt.pie(cases_dist, labels = [case.name for case in DelayedCases], autopct='%1.1f%%', shadow=True)
    # plt.title(f'Distribution of delayed hits Cases in windows of size {args.window_size}')
    plt.savefig(f'{DIR}/{args.dir}/latencies_dist_{args.window_size}.png', dpi=300)
    plt.close()
    
    with open(f'{DIR}/{args.dir}/item_dists_{args.window_size}.pickle', 'wb') as f:
        pickle.dump(items_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
        
    with open(f'{DIR}/{args.dir}/results_{args.window_size}.pickle', 'wb') as f:
        pickle.dump({'num of hits dist' : dist, 'delayed latency dist' : delayed_latency_dist}, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    main()