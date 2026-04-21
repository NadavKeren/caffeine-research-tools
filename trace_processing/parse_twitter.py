import csv
import re
import xxhash
import argparse
from pathlib import Path
from itertools import islice
from typing import List, Iterator
from rich import print, pretty
pretty.install()

# Shortening the traces to be either 200mil requests or 10 hours long.
MAX_REQUESTS_TO_PROCESS = 200000000
MAX_TIME_TO_PROCESS = 36000


def read_batches(reader: csv.DictReader, batch_size: int) -> Iterator[List[dict]]:
    while True:
        batch = list(islice(reader, batch_size))
        if not batch:
            break
        yield batch


def process_key(key: str) -> int:
    """
        Extract the last part of key and hash it to int64.
        The key is of the format: q:q:1:8WTwl7huJeQ, thus, we hash only its end.
    """
    key_suffix = key.split(':')[-1]
    hash_value = xxhash.xxh3_64_intdigest(key_suffix.encode('utf-8'))
    return hash_value


def process_csv_batches(input_file: Path, output_file: Path, batch_size: int = 10000) -> None:
    processed_count = 0
    total_filtered = 0
    batch_number = 0
    last_timestamp = 0
    
    with input_file.open('r', newline='', encoding='utf-8') as infile, \
         output_file.open('w', newline='', encoding='utf-8') as outfile:
             
        fieldnames = ['timestamp', 'key', 'key_size', 'value_size', 'client_id', 'operation', 'TTL']
        reader = csv.DictReader(infile, fieldnames=fieldnames)
        writer = csv.writer(outfile, delimiter=' ')
        
        for batch in read_batches(reader, batch_size):
            batch_number += 1
            filtered_rows = []
            for row in batch:
                operation = row.get('operation', '').strip().lower()
                if operation in ['get', 'gets']:
                    timestamp = int(row.get('timestamp', '').strip()) * 1000  # Convert to milliseconds
                    last_timestamp = int(timestamp)
                    key = row.get('key', '').strip()
                    hashed_key = process_key(key)
                    filtered_rows.append([timestamp, hashed_key])
                    total_filtered += 1
            
            if filtered_rows:
                writer.writerows(filtered_rows)
            
            processed_count += len(batch)
            if (batch_number % 100 == 0):
                print(f"Processed {processed_count} rows, filtered {total_filtered} rows so far...")
                
            if (last_timestamp > MAX_TIME_TO_PROCESS or total_filtered > MAX_REQUESTS_TO_PROCESS):
                break
    
    print(f"\nCompleted processing:")
    print(f"Total rows processed: {processed_count}")
    print(f"Total rows written to output: {total_filtered}")
    print(f"The last timestamp written is: {last_timestamp}")
    print(f"Kept {(total_filtered) / processed_count * 100}% of the lines")
    print(f"Output file: {output_file}")


def extract_cluster_number(filename: str) -> str | None:
    match = re.search(r'cluster(\d{2})', filename)
    if match:
        return match.group(1)
    return None


def main():
    parser = argparse.ArgumentParser(description='Parse Twitter trace file')
    parser.add_argument('-i', '--input-file', required=True, type=str, help='Path to input trace file')
    parser.add_argument('-o', '--output-path', required=True, type=str, help='Output directory path')
    parser.add_argument('--batch-size', type=int, required=False, default=10000,
                        help='Number of rows to process at a time (default: 10000)')

    args = parser.parse_args()

    input_file = Path(args.input_file)
    if not input_file.exists() or not input_file.is_file():
        print(f"[bold red]Error: Input file '{args.input_file}' does not exist")
        exit(1)

    cluster_number = extract_cluster_number(input_file.name)
    if cluster_number is None:
        print(f'[bold red]Error: Cannot extract cluster number from filename {input_file.name}')
        print(f'[yellow]Expected format: clusterXX...')
        exit(1)

    output_dir = Path(args.output_path)
    output_filename = f'twitter{cluster_number}.trace'
    output_file = output_dir / output_filename

    print(f'Input file: {str(input_file.resolve())}')
    print(f'Output file: {str(output_file.resolve())}')

    output_dir.mkdir(parents=True, exist_ok=True)

    process_csv_batches(input_file, output_file, args.batch_size)

    print(f'[green]Done processing: [purple]{input_file.name} -> [cyan]{output_filename}')


if __name__ == "__main__":
    main()
