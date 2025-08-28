import argparse

from pathlib import Path
from os import SEEK_END, SEEK_CUR

from typing import List, Dict, TextIO

from itertools import islice

from rich import print, pretty
from rich.progress import Progress
pretty.install()

CURR_EMPTY_ID = 0
TIMEFRAME = 0


def read_last_line(file_path: Path) -> str:
    with file_path.open('rb') as f:
        try:  # catch OSError in case of a one line file 
            f.seek(-2, SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, SEEK_CUR)
        except OSError:
            f.seek(0)
        last_line = f.readline().decode()
    
    return last_line


def calculate_timeframe(file_path: Path) -> None:
    global TIMEFRAME
    with file_path.open('r') as file:
        first_line = file.readline()
    
    last_line = read_last_line(file_path) # more efficient for larger files
    
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

def pad_file(file_path : Path, num_of_times : int):
    output_file = file_path.parent / f'{file_path.stem}x{num_of_times + 1}.trace'
    
    calculate_timeframe(file_path)
    
    with Progress() as progress:
        write_progress = progress.add_task('[bold #bedcfe]Times written', total=num_of_times, start=True)
        with file_path.open('r') as origin_file, output_file.open('w') as output_file:
            for i in range(num_of_times + 1):
                read_and_replace_ids(origin_file, output_file, i)
                origin_file.seek(0)
                progress.update(write_progress, advance=1)
                        
        print(f'[yellow]New number of unqiue items is {CURR_EMPTY_ID}, timeframe: {TIMEFRAME}')


def main():
    parser = argparse.ArgumentParser(
        description='This script takes an existing file and re-runs it <times> + 1 times in order to create a longer trace'
    )
    
    parser.add_argument('-f', '--file', help='The file to be padded, does not change the original', type=str, required=True)
    parser.add_argument('-t', '--times', help='Number of times to pad the file (1 -> doubles the file, 2 -> triples...)', type=int, required=True)
    
    args = parser.parse_args()
    print(f'Given args: {args}')
    
    file_path = Path(args.file)
    if not file_path.exists():
        print(f'[red bold]Error: No such file: {args.file}')
        exit(1)
    
    if not file_path.is_file():
        print(f'[red bold]Error: {args.file} is not a file')
        exit(1)
        
    if (args.times <= 0):
        print(f'[red bold]Error: bad number of times {args.times}')
        exit(1)
        
    pad_file(file_path, args.times)
    
    
if __name__ == '__main__':
    main()