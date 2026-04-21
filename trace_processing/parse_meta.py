import argparse
import re
from pathlib import Path

from rich import pretty, print

pretty.install()

def parseLine(entry: str) -> str | None:
    parts = entry.strip().split(',')

    if len(parts) != 8:
        return None

    timestamp = parts[0]
    key = parts[1]
    op = parts[3]

    if 'GET' in op.upper():
        return f'{timestamp} {key}'
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

def extract_metakv_version(filename: str) -> str | None:
    match = re.search(r'metakv([24])', filename)
    if match:
        return match.group(1)

    if '202210_kv_' in filename:
        return '2'
    if '202401_kv_' in filename:
        return '4'

    return None


def main():
    parser = argparse.ArgumentParser(description='Parse metaKV trace file')
    parser.add_argument('-i', '--input-file', help='Input trace file', type=str, required=True)
    parser.add_argument('-o', '--output-path', help='Output directory path', type=str, required=True)

    args = parser.parse_args()

    input_file = Path(args.input_file)
    output_dir = Path(args.output_path)

    if not input_file.exists():
        print(f'[bold red]Error: Input file {input_file} does not exist')
        return

    version = extract_metakv_version(input_file.name)
    if version is None:
        print(f'[bold red]Error: Cannot extract metakv version from filename {input_file.name}')
        print(f'[yellow]Expected format: metakvX-... or 202210_kv_... or 202401_kv_...')
        return

    output_filename = f'metakv{version}.trace'
    output_file = output_dir / output_filename

    print(f'Input file: {str(input_file.resolve())}')
    print(f'Output file: {str(output_file.resolve())}')

    output_dir.mkdir(exist_ok=True, parents=True)

    print(f'[orange]Start processing [purple]{input_file.name}')
    processFile(input_file, output_file)
    print(f'[green]Done processing: [purple]{input_file.name} -> [cyan]{output_filename}')


if __name__ == '__main__':
    main()
