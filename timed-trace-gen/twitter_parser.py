from os.path import *
from os import path, listdir, makedirs
import re

from typing import List

from utils import Timer, Colors

INPUT_DIR = '/home/nadav/caching/traces/twitter/'
OUTPUT_DIR = './twitter_processed'
items = {}
id = 0

def parseLine(entry: str):   
    global id
    global items
    splitted_line = entry.split(',')
    
    try:
        time = int(float(splitted_line[0]) * 1000)
        object_str = splitted_line[1]

        if object_str not in items:
            items[object_str] = id
            id += 1
        object_id = items[object_str]
        
        return f'{time} {object_id}'
    except Exception as error:
        return None

def _processFile(fname: str):
    global id
    global items
    with open(f'{INPUT_DIR}/{fname}', encoding='utf-8',errors='replace') as raw_file:
        with open(f'{OUTPUT_DIR}/{fname}','w') as output_file:
            line = raw_file.readline()
            while line:
                output_line = parseLine(line)
                if output_line != None:
                    output_file.write(f'{output_line}\n')
                line = raw_file.readline()
            
            print(f'Number of items: {len(items)}, max ID: {id}')           
            
            items = items.clear()
            id = 0
                
def processFiles(files : List[str]):
    print(f'{Colors.bold}{Colors.yellow}Processing the files: {Colors.cyan}{files}{Colors.reset}\n')
    for file in files:
        if not path.exists(f'{OUTPUT_DIR}/{file}'):
            print(f'{Colors.orange}Start processing {Colors.purple}{file}{Colors.reset}')
            with Timer():
                _processFile(file)
                
            print(f'{Colors.green}Done processing: {Colors.purple}{file}{Colors.reset}')
    print(f'{Colors.bold}{Colors.cyan}Done processing files{Colors.reset}\n\n')
    
    
def main():
    input_files_paths = [f for f in listdir(INPUT_DIR) 
                         if isfile(join(INPUT_DIR, f)) 
                         and re.fullmatch("cluster[0-9]+", f)]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    processFiles(input_files_paths)

     
if __name__ == '__main__':
    main()
