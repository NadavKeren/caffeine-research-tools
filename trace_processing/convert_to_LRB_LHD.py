import argparse
from pathlib import Path

from typing import List

from rich import print, pretty
from rich.progress import Progress
pretty.install()

MAX_INT64 = 2 ** 63 - 1

def _parseLine(entry: str) -> str:
    splitted_line = entry.split(' ')
    
    time = splitted_line[0]
    object_id = splitted_line[1]
    object_id = int(object_id, 16) & MAX_INT64
    
    return f"{time} {object_id} 1"


def _processFile(file: Path, output_path: Path, progress: Progress) -> None:
    with file.open('r', encoding='utf-8',errors='replace') as original_format_file:
        lines_processed = 0
        with output_path.open('w') as LRB_format_file:
            line = original_format_file.readline()
            while line:
                lines_processed += 1
                output_line = _parseLine(line)
                LRB_format_file.write(f'{output_line}\n')
                line = original_format_file.readline()
        progress.console.print(f"[green]Processed {lines_processed} lines")

                
def processFiles(files : List[Path], output_dir: Path):
    with Progress() as progress:
        files_progress = progress.add_task('[bold #bedcfe]Files processed', total=len(files), start=True)
        progress.console.print(f'[bold yellow]Processing the files: [bold cyan]{files}\n')
        for file in files:
            output_path = output_dir / f'{file.stem}-LRB.trace'
            print(output_path.resolve())
            if not output_path.exists():
                progress.console.print(f'[orange]Start processing [purple]{file}')
                _processFile(file, output_path, progress)

                progress.console.print(f'[green]Done processing: [purple]{file}')
                progress.update(files_progress, advance=1)

    print(f'[bold cyan]Done processing files\n\n')
    
    
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help="The input dir of all the files to convert, \
                        each line needs to be space-seperated with the two first parts: timestamp key", type=str, required=True)

    args = parser.parse_args()
    input_dir = Path(args.input)

    if not input_dir.exists() or not input_dir.is_dir():
        print("[red bold]Error: invalid input dir")

    input_files_paths = [f for f in input_dir.iterdir() if f.is_file()]

    output_dir = input_dir / 'object_storage_LRB'
    output_dir.mkdir(exist_ok=True)

    print(f'Writing output to: {str(output_dir.resolve())}')

    processFiles(input_files_paths, output_dir)

     
if __name__ == '__main__':
    main()
