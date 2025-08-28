from os.path import *
from os import path, listdir, makedirs

from typing import List

from rich import print, pretty
from rich.progress import Progress
pretty.install()

INPUT_DIR = './object_storage_processed'
OUTPUT_DIR = './object_storage_LRB'

MAX_INT64 = 2 ** 63 - 1

def _parseLine(entry: str) -> str:
    splitted_line = entry.split(' ')
    
    time = splitted_line[0]
    object_id = splitted_line[1]
    object_id = int(object_id, 16) & MAX_INT64
    
    return f"{time} {object_id} 1"


def _processFile(fname: str, progress: Progress) -> None:
    with open(f'{INPUT_DIR}/{fname}', encoding='utf-8',errors='replace') as original_format_file:
        lines_processed = 0
        lines_removed = 0
        with open(f'{OUTPUT_DIR}/{fname}','w') as LRB_format_file:
            line = original_format_file.readline()
            while line:
                lines_processed += 1
                output_line = _parseLine(line)
                LRB_format_file.write(f'{output_line}\n')
                line = original_format_file.readline()
        progress.console.print(f"[green]Processed {lines_processed} lines")

                
def processFiles(files : List[str]):
    with Progress() as progress:
        files_progress = progress.add_task('[bold #bedcfe]Files processed', total=len(files), start=True)
        progress.console.print(f'[bold yellow]Processing the files: [bold cyan]{files}\n')
        for file in files:
            if not path.exists(f'{OUTPUT_DIR}/{file}'):
                progress.console.print(f'[orange]Start processing [purple]{file}')
                _processFile(file, progress)
                    
                progress.console.print(f'[green]Done processing: [purple]{file}')
                progress.update(files_progress, advance=1)
                
    print(f'[bold cyan]Done processing files\n\n')
    
    
def main():
    input_files_paths = [f for f in listdir(INPUT_DIR) 
                         if isfile(join(INPUT_DIR, f))]
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    processFiles(input_files_paths)

     
if __name__ == '__main__':
    main()
