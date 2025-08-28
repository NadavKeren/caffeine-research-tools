import argparse
import re

from rich import pretty
from rich.console import Console
from rich.progress import Progress

from pathlib import Path
from os import SEEK_END

from typing import List

from itertools import islice

from latency_generators import *
from utils import *

CONSOLE = Console()
pretty.install()


def get_trace_name(fname: str):
    temp_fname = fname.lower()
    name = re.findall('trace0[0-9][0-9]', temp_fname)
    
    return name[0]


def read_last_line(file_path: Path):
    path = Path(file_path)
    with path.open('rb') as file:
        file.seek(0, SEEK_END)
        pos = file.tell() - 2
        while pos >= 0:
            file.seek(pos)
            if file.read(1) == b'\n':
                break
            pos -= 1
        last_line = file.readline().decode()
    return last_line


def calculate_file_start_and_end_times(file_path: Path) -> (int, int):
    with file_path.open('r') as file:
        first_line = file.readline()
    
    last_line = read_last_line(file_path) # more efficient for larger files
    
    start_time = first_line.split(' ')[0]
    end_time = last_line.split(' ')[0]
    
    return int(start_time), int(end_time)


def changeTimestampsAndWriteToFile(input_files: List[Path], output_path: Path):
    last_file_end = 0
    
    timestamp = 1
    
    
    file_ends = list()
    
    with output_path.open('w') as outputFile, Progress() as progress:
        file_progress = progress.add_task('[bold #bedcfe]Files added', total=len(input_files), start=True)
        for input_file in input_files:
            with input_file.open('r') as file_reader:
                num_of_lines = 0
                BATCH_SIZE = 10000
                lines = list(islice(file_reader, BATCH_SIZE))
                
                file_start, file_end = calculate_file_start_and_end_times(input_file)
                progress.console.print((file_start, file_end))
                
                while (lines) :
                    for line in lines:
                        written_time, key, hit_penalty, miss_penalty = line.split(' ')
                        written_time = int(written_time)
                        key = int(key)
                        hit_penalty = int(hit_penalty)
                        miss_penalty = int(miss_penalty.strip(' \n'))
                            
                        timestamp = written_time - file_start + last_file_end + 1
                        num_of_lines += 1
                            
                            
                        outputFile.write(f'{timestamp} {key} {hit_penalty} {miss_penalty}\n')
                        
                    lines = list(islice(file_reader, BATCH_SIZE))
                
                last_file_end = last_file_end + file_end - file_start
                file_ends.append((str(input_file), last_file_end, num_of_lines))
                
                progress.update(file_progress, advance=1)
                
    CONSOLE.print(file_ends)
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', help='The directory containing the original trace', type=str, required=True)
    parser.add_argument('--traces', help='The traces to merge', type=str, nargs='+', required=True)
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = input_dir / "merged"
    output_dir.mkdir(exist_ok=True)
    
    trace_names = [get_trace_name(trace) for trace in args.traces]
    input_files = [input_dir / trace for trace in args.traces]
    
    CONSOLE.print(args)
    CONSOLE.print(trace_names)
    CONSOLE.print(input_files)
    
    for file in input_files:
        start_time, end_time = calculate_file_start_and_end_times(file)
        CONSOLE.print(f'{str(file)}: {end_time - start_time}')
    

    setname = "IBMOS-" + "-".join(trace_names) + ".trace"
    output_path = output_dir / setname
    changeTimestampsAndWriteToFile(input_files, output_path)
    
    CONSOLE.log("[bold #a3b18a]Done\n#####################\n\n")

                    
if __name__ == '__main__':
    main()