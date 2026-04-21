import argparse
import re
from pathlib import Path

from rich import pretty, print
pretty.install()

def parseLine(entry: str) -> str | None:
    splitted_line = entry.split(' ')

    time = splitted_line[0]
    cmd = splitted_line[1]
    object_id = splitted_line[2]

    if not 'DELETE' in cmd and not 'SET' in cmd:
        return f'{time} {object_id}'
    else:
        return None

def processFile(input_path: Path, output_path: Path) -> None:
    with input_path.open(encoding='utf-8', errors='replace') as raw_file:
        lines_processed = 0
        lines_removed = 0
        with output_path.open('w') as output_file:
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

def extract_trace_number(filename: str) -> str | None:
    """Extract trace number from IBM filename format: IBMObjectStoreTraceXXXPart0 -> XXX"""
    match = re.search(r'IBMObjectStoreTrace(\d{3})Part0', filename)
    if match:
        return match.group(1)
    return None


def main():
    parser = argparse.ArgumentParser(description='Parse IBM Object Storage trace file')
    parser.add_argument('-i', '--input-file', help='Input trace file', type=str, required=True)
    parser.add_argument('-o', '--output-path', help='Output directory path', type=str, required=True)

    args = parser.parse_args()

    input_file = Path(args.input_file)
    output_dir = Path(args.output_path)

    if not input_file.exists():
        print(f'[bold red]Error: Input file {input_file} does not exist')
        return

    # Extract trace number from filename
    trace_number = extract_trace_number(input_file.name)
    if trace_number is None:
        print(f'[bold red]Error: Cannot extract trace number from filename {input_file.name}')
        print(f'[yellow]Expected format: IBMObjectStoreTraceXXXPart0')
        return

    # Generate output filename
    output_filename = f'IBM{trace_number}.trace'
    output_file = output_dir / output_filename

    print(f'Input file: {str(input_file.resolve())}')
    print(f'Output file: {str(output_file.resolve())}')

    output_dir.mkdir(exist_ok=True, parents=True)

    print(f'[orange]Start processing [purple]{input_file.name}')
    processFile(input_file, output_file)
    print(f'[green]Done processing: [purple]{input_file.name} -> [cyan]{output_filename}')

     
if __name__ == '__main__':
    main()
