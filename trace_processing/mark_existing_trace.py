import argparse
import sys
from rich import pretty, print
from pathlib import Path
from itertools import islice
from typing import Iterator, List

pretty.install()
BATCH_SIZE = 10_000

def batched_file_reader(file_path: Path) -> Iterator[List[str]]:
    with open(file_path, 'r', encoding='utf-8') as f:
        while True:
            batch = list(islice(f, BATCH_SIZE))
            if not batch:
                break
            
            batch = [line.strip() for line in batch]
            if batch:
                yield batch


def process_batches(trace_file: Path, marked_file: Path, output_path: Path, multiplier: int) -> None:
    with open(output_path, 'w', encoding='utf-8') as output_file:
        trace_file_batches = batched_file_reader(trace_file)
        marked_file_batches = batched_file_reader(marked_file)
        
        total_processed = 0
        batch_num = 0
        
        for batch1, batch2 in zip(trace_file_batches, marked_file_batches):
            batch_num += 1
            
            if len(batch1) != len(batch2):
                print(f"[red bold]Error: Batch {batch_num} size mismatch - trace_file: {len(batch1)}, marked_file: {len(batch2)}")
                exit(1)
            
            for line1, line2 in zip(batch1, batch2):
                line_num = total_processed + 1
                time1, id1, miss_penalty = line1.split()
                time2, _, is_hit = line2.split()
                time1 = int(time1)
                time2 = int(time2)
                time2 *= multiplier
                
                if time1 != time2:
                    print(f"[bold red]Error: Line {line_num} - timestamp mismatch: {time1} != {time2}")
                    exit(1)
                
                merged_line = f"{time1} {id1} {miss_penalty} {is_hit}\n"
                output_file.write(merged_line)
                
                total_processed += 1
            
            if batch_num % 100 == 0:
                print(f"[yellow]Processed {total_processed:_} lines...")
        
        remaining1 = list(islice(trace_file_batches, 1))
        remaining2 = list(islice(marked_file_batches, 1))
        
        if remaining1:
            print(f"[bold red]Error: The trace file has more lines after line {total_processed:_}")
            exit(1)
        if remaining2:
            print(f"[bold red]Error: The marked file has more lines after line {total_processed:_}")
            exit(1)
    
    print(f"[bold green]Successfully processed {total_processed:_} lines.")
    print(f"[bold cyan]Output written to: {output_path}")


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--trace_file', '-t', type=str, required=True,
                        help='Path to latency appended file (format: request-time item-id hit-penalty miss-penalty)')
    
    parser.add_argument('--marked_file', '-m', type=str, required=True,
                        help='Path to marked trace file (format: request-time item-id is-hit)')
    
    parser.add_argument('--output', '-o', type=str, required=True, help='Path to output marked and latency appended file')
    args = parser.parse_args()
    
    trace_file = Path(args.trace_file)
    marked_file_path = Path(args.marked_file)
    output_path = Path(args.output)
    
    if not trace_file.exists():
        print(f"Error: trace_file does not exist: {trace_file}")
        sys.exit(1)
    
    # multiplier = 1 if not marked_file_path.stem.startswith("LRB") else 1000
    # multiplier = 1000
    multiplier = 1
    if not marked_file_path.exists():
        print(f"Error: marked_file does not exist: {marked_file_path}")
        sys.exit(1)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Input file 1: {trace_file}")
    print(f"Input file 2: {marked_file_path}")
    print(f"Output file: {output_path}")
    
    process_batches(trace_file, marked_file_path, output_path, multiplier)


if __name__ == "__main__":
    main()