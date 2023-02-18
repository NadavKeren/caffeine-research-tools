from os.path import *
from os import path, listdir, makedirs

from typing import List

from utils import Timer, Colors

INPUT_DIR = './object_storage_raw'
OUTPUT_DIR = './object_storage_processed'

def parseLine(entry: str):   
    splitted_line = entry.split(' ')
    
    time = splitted_line[0]
    cmd = splitted_line[1]
    object_id = splitted_line[2]
    
    if not 'DELETE' in cmd:
        return f'{time} {object_id}'

def _processFile(fname: str):
    with open(f'{INPUT_DIR}/{fname}', encoding='utf-8',errors='replace') as raw_file:
        with open(f'{OUTPUT_DIR}/{fname}','w') as output_file:
            line = raw_file.readline()
            while line:
                output_line = parseLine(line)
                if output_line != None:
                    output_file.write(f'{output_line}\n')
                line = raw_file.readline()
                
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
                         and f.startswith('IBMObjectStore')]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    processFiles(input_files_paths)

     
if __name__ == '__main__':
    main()
