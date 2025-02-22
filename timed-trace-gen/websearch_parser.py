from os.path import *
from os import path, listdir, makedirs

from typing import List, Tuple

from utils import Timer

#* The format of each line is (non-important), Address, Size, r/w, Timestamp (seconds), (non important...) 
#* We transform it to sector aligned blocks with timestamps in mili-seconds resolution

SECTOR_SIZE = 4096
OUTPUT_DIR = './processed'
OUTPUT_FILE_PREFIX = f'{OUTPUT_DIR}/processed_'

# Returning a list of **sector aligned** items of format: (timestamp, address)
def parseLine(entry: str) -> List[Tuple[int, int]]:
    _, start_address, size, _, timestamp, *_ = entry.split(",")
    start_address = int(start_address)
    size = int(size)
    end_address = start_address + size
    timestamp = int(1000 * float(timestamp))
    
    aligned_start = (start_address // SECTOR_SIZE) * SECTOR_SIZE
    curr = aligned_start
    
    lst = []
    
    while curr < end_address:
        lst.append((timestamp, curr))
        curr += SECTOR_SIZE
    
    return lst


def _processFile(fname: str):
    with open(f'./input/{fname}', encoding='utf-8',errors='replace') as inputFile:
        with open(f'{OUTPUT_FILE_PREFIX}{fname}','w') as outputFile:
            line = inputFile.readline().replace('\n', '')
            count = 1
            while line:
                lst = parseLine(line)
                for timestamp, address in lst:
                    outputFile.write(f'{timestamp} {address}\n')
                line = inputFile.readline().replace('\n', '')
                count += 1
                
def processFiles(files : List[str]):
    print(f'processing the files: {files}\n')
    for file in files:
        if not path.exists(f'{OUTPUT_FILE_PREFIX}{file}'):
            print(f'start processing {file}')
            with Timer():
                _processFile(file)
                
            print(f'done processing: {file}')
    print('done processing files\n\n')
    
def main():
    input_files_paths = [f for f in listdir('./input') if isfile(join('./input', f)) and f.startswith('WebSearch')]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    processFiles(input_files_paths)
    
if __name__ == '__main__':
    main()
