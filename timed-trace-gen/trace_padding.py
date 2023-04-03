import numpy as np 

import argparse
import datetime
import tqdm
import re
import pprint

from os.path import *
from os import path, listdir, makedirs, SEEK_END, SEEK_CUR

from typing import List, Dict, TextIO

from itertools import chain, islice

from utils import *


CURR_EMPTY_ID = 0
TIMEFRAME = 0


def read_last_line(fname: str) -> str:
    with open(fname, 'rb') as f:
        try:  # catch OSError in case of a one line file 
            f.seek(-2, SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, SEEK_CUR)
        except OSError:
            f.seek(0)
        last_line = f.readline().decode()
    
    return last_line


def calculate_timeframe(fname: str) -> None:
    global TIMEFRAME
    with open(fname, 'r') as file:
        first_line = file.readline()
    
    last_line = read_last_line(fname) # more efficient for larger files
    
    start_time = first_line.split(' ')[0]
    end_time = last_line.split(' ')[0]
    
    TIMEFRAME = int(end_time) - int(start_time)


def read_and_replace_ids(input_file: TextIO, output_file: TextIO, num_of_timeframes: int):
    global CURR_EMPTY_ID
    ids = dict()
    
    BATCH_SIZE = 10000
    lines = [line for line in islice(input_file, 0, BATCH_SIZE)]
    
    while(lines):
        for line in lines:
            splitted_line = line.split(' ')
            curr_timestamp = int(splitted_line[0]) + TIMEFRAME * num_of_timeframes + 1
            key = splitted_line[1]
            hit_penalty = splitted_line[2]
            delay = splitted_line[3]
            
            new_id = ids.get(key)
            
            if new_id is None:
                new_id = CURR_EMPTY_ID
                CURR_EMPTY_ID += 1
                ids[key] = new_id
            
            output_file.write(f'{curr_timestamp} {new_id} {hit_penalty} {delay}')
    
        lines = [line for line in islice(input_file, 0, BATCH_SIZE)]
    
    ids.clear()

def pad_file(dir : str, fname : str, num_of_times : int):
    base_fname = fname.rstrip('.trace')
    output_file_name = f'{dir}/{base_fname}x{num_of_times + 1}.trace'
    
    calculate_timeframe(f'{dir}/{fname}')
    
    with Timer():
        with open(f'{dir}/{fname}', 'r') as origin_file:
            with open(output_file_name, 'w') as output_file:
                for i in tqdm.trange(num_of_times + 1):
                    read_and_replace_ids(origin_file, output_file, i)
                    origin_file.seek(0)
                    
        print(f'{Colors.bold}{Colors.yellow}New number of unqiue items is {CURR_EMPTY_ID}, timeframe: {TIMEFRAME}{Colors.reset}')


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d', '--directory', help='The directory of the input and expected output files', type=str, required=True)
    parser.add_argument('-f', '--file', help='The file to be padded, does not change the original', type=str, required=True)
    parser.add_argument('-t', '--times', help='Number of times to pad the file (1 -> doubles the file, 2 -> triples...)', type=int, required=True)
    
    args = parser.parse_args()
    print(f'Given args: {pprint.pprint(args)}')
    
    pad_file(args.directory, args.file, args.times)
    
    
if __name__ == '__main__':
    main()