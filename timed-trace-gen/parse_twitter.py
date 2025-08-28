import csv
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
    """Extract last part of key and hash it to int64."""
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
                    timestamp = row.get('timestamp', '').strip()
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', required=True, type=str, help='Path to input trace file')
    parser.add_argument('-o', '--output_file', required=True, type=str, help='Path to output filtered trace'
    )
    parser.add_argument('--batch-size', type=int, required=False, default=10000, 
                        help='Number of rows to process at a time (default: 10000)')
    
    args = parser.parse_args()
    
    print(args)
    
    input_file = Path(args.input_file)
    if not input_file.exists() or not input_file.is_file():
        print(f"Error: Input file '{args.input_file}' is invalid")
        exit(1)
    
    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
        
    process_csv_batches(input_file, output_file, args.batch_size)


if __name__ == "__main__":
    main()
