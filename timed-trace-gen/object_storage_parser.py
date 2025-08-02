from os.path import *
from os import path, listdir, makedirs

from typing import List

from rich import print

INPUT_DIR = './object_storage_raw'
OUTPUT_DIR = './object_storage_processed'

def parseLine(entry: str) -> str | None:
    splitted_line = entry.split(' ')
    
    time = splitted_line[0]
    cmd = splitted_line[1]
    object_id = splitted_line[2]
    
    if not 'DELETE' in cmd and not 'SET' in cmd:
        return f'{time} {object_id}'
    else:
        return None

def _processFile(fname: str) -> None:
    with open(f'{INPUT_DIR}/{fname}', encoding='utf-8',errors='replace') as raw_file:
        lines_processed = 0
        lines_removed = 0
        with open(f'{OUTPUT_DIR}/{fname}','w') as output_file:
            line = raw_file.readline()
            while line:
                lines_processed += 1
                output_line = parseLine(line)
                if output_line != None:
                    output_file.write(f'{output_line}\n')
                else:
                    lines_removed += 1
                line = raw_file.readline()
        print(f"[green]Processed {lines_processed} lines ignoring [yellow]{lines_removed}")

                
def processFiles(files : List[str]):
    print(f'[bold yellow]Processing the files: [bold cyan]{files}\n')
    for file in files:
        if not path.exists(f'{OUTPUT_DIR}/{file}'):
            print(f'[orange]Start processing [purple]{file}')
            _processFile(file)
                
            print(f'[green]Done processing: [purple]{file}')
    print(f'[bold cyan]Done processing files\n\n')
    
    
def main():
    input_files_paths = [f for f in listdir(INPUT_DIR) 
                         if isfile(join(INPUT_DIR, f)) 
                         and f.startswith('IBMObjectStore')]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    processFiles(input_files_paths)

     
if __name__ == '__main__':
    main()
