from os.path import *
from os import path, listdir, makedirs

import argparse

from typing import List, Tuple

from utils import Timer

#* The format of each line is (non-important), Address, Size, r/w, Timestamp (seconds), (non important...) 
#* We transform it to sector aligned blocks with timestamps in mili-seconds resolution

SECTOR_SIZE = 4096
OUTPUT_DIR = './processed'

# Returning a list of items of format: (timestamp, address), the items are already in sectors
def parseLine(entry: str) -> List[Tuple[int, int]]:
    try:
        _, _, _, timestamp, _, _, _, start_address, _, size, *_ = entry.split()
        start_address = int(start_address)
        size = int(size)
        end_address = start_address + size
        timestamp = int(1000 * float(timestamp))
    
        return [(timestamp, start_address + i) for i in range(size)]
    except: # all the bad lines with no addresses
        return None


def _processFile(fname: str, INPUT_DIR: str, OUTPUT_DIR: str) -> None:
    with open(f'{INPUT_DIR}/{fname}', encoding='utf-8',errors='replace') as inputFile:
        with open(f'{OUTPUT_DIR}/{fname}','w') as outputFile:
            line = inputFile.readline().replace('\n', '')
            count = 1
            while line:
                lst = parseLine(line)
                if lst is not None:
                    for timestamp, address in lst:
                        outputFile.write(f'{timestamp} {address}\n')
                line = inputFile.readline().replace('\n', '')
                count += 1
                
def processFiles(files : List[str], INPUT_DIR: str, OUTPUT_DIR: str) -> None:
    print(f'processing the files: {files}\n')
    for file in files:
        if not path.exists(f'{OUTPUT_DIR}/{file}'):
            print(f'start processing {file}')
            with Timer():
                _processFile(file, INPUT_DIR, OUTPUT_DIR)
                
            print(f'done processing: {file}')
    print('done processing files\n\n')
    
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--input', '-i', help='Input directory containing the traces as CSVs grouped together into trace file (.trace)', type=str,  required=True)
    
    args = parser.parse_args()
    
    input_files_paths = [f for f in listdir(args.input) if isfile(join(args.input, f)) and f.endswith('.trace')]
    
    OUTPUT_DIR = f'{args.input}/processed'
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    processFiles(input_files_paths, args.input, OUTPUT_DIR)
    
if __name__ == '__main__':
    main()
