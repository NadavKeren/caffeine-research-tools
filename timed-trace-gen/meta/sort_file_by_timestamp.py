import os
import tempfile
import heapq
import argparse
from pathlib import Path

class TimestampRecord:
    def __init__(self, line):
        self.line = line.strip()
        parts = self.line.split(' ', 1)
        if len(parts) < 2:
            raise ValueError(f"Invalid line format: {self.line}")
        
        try:
            # Try float first, then int
            self.timestamp = float(parts[0])
        except ValueError:
            self.timestamp = int(parts[0])
        
        self.key = parts[1]
    
    def __lt__(self, other):
        return self.timestamp < other.timestamp
    
    def __str__(self):
        return self.line

def split_file(input_file, chunk_size_mb=100):
    """Split input file into sorted chunks that fit in memory"""
    chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
    chunk_files = []
    temp_dir = tempfile.mkdtemp()
    
    print(f"Splitting file into chunks of ~{chunk_size_mb}MB...")
    
    with open(input_file, 'r') as f:
        chunk_num = 0
        
        while True:
            records = []
            current_size = 0
            
            # Read chunk
            for line in f:
                line_size = len(line.encode('utf-8'))
                if current_size + line_size > chunk_size and records:
                    break
                
                try:
                    record = TimestampRecord(line)
                    records.append(record)
                    current_size += line_size
                except ValueError as e:
                    print(f"Warning: Skipping invalid line: {e}")
                    continue
            
            if not records:
                break
            
            # Sort chunk in memory
            records.sort()
            
            # Write sorted chunk to temporary file
            chunk_file = os.path.join(temp_dir, f"chunk_{chunk_num}.txt")
            with open(chunk_file, 'w') as chunk_f:
                for record in records:
                    chunk_f.write(str(record) + '\n')
            
            chunk_files.append(chunk_file)
            chunk_num += 1
            print(f"Created chunk {chunk_num} with {len(records):,} records")
    
    return chunk_files, temp_dir

def merge_chunks(chunk_files, output_file):
    """Merge sorted chunks using a min-heap"""
    print(f"Merging {len(chunk_files)} chunks...")
    
    # Open all chunk files
    chunk_readers = []
    heap = []
    
    for i, chunk_file in enumerate(chunk_files):
        reader = open(chunk_file, 'r')
        chunk_readers.append(reader)
        
        # Read first line from each chunk
        line = reader.readline()
        if line:
            try:
                record = TimestampRecord(line)
                heapq.heappush(heap, (record, i))
            except ValueError:
                continue
    
    # Merge using heap
    with open(output_file, 'w') as output:
        total_written = 0
        
        while heap:
            record, chunk_idx = heapq.heappop(heap)
            output.write(str(record) + '\n')
            total_written += 1
            
            if total_written % 1000000 == 0:
                print(f"Merged {total_written:,} records...")
            
            # Read next line from same chunk
            next_line = chunk_readers[chunk_idx].readline()
            if next_line:
                try:
                    next_record = TimestampRecord(next_line)
                    heapq.heappush(heap, (next_record, chunk_idx))
                except ValueError:
                    continue
    
    # Close all readers
    for reader in chunk_readers:
        reader.close()
    
    print(f"Merge complete. Total records written: {total_written:,}")

def cleanup_temp_files(chunk_files, temp_dir):
    """Remove temporary files and directory"""
    for chunk_file in chunk_files:
        try:
            os.remove(chunk_file)
        except OSError:
            pass
    
    try:
        os.rmdir(temp_dir)
    except OSError:
        pass

def sort_large_file(input_file, output_file, chunk_size_mb=100):
    """Main function to sort a large file using external merge sort"""
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"Error: Input file '{input_file}' not found.")
        return False
    
    file_size_mb = input_path.stat().st_size / (1024 * 1024)
    print(f"Sorting file: {input_file} ({file_size_mb:.1f}MB)")
    print(f"Output file: {output_file}")
    
    try:
        # Step 1: Split file into sorted chunks
        chunk_files, temp_dir = split_file(input_file, chunk_size_mb)
        
        if not chunk_files:
            print("No valid records found in input file.")
            return False
        
        # Step 2: Merge chunks
        merge_chunks(chunk_files, output_file)
        
        # Step 3: Cleanup
        cleanup_temp_files(chunk_files, temp_dir)
        
        print("✓ Sorting completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error during sorting: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Sort large files by timestamp using external merge sort')
    parser.add_argument('input_file', help='Input file to sort')
    parser.add_argument('output_file', help='Output file for sorted data')
    parser.add_argument('--chunk-size', type=int, default=100, 
                       help='Chunk size in MB (default: 100MB)')
    
    args = parser.parse_args()
    
    success = sort_large_file(args.input_file, args.output_file, args.chunk_size)
    
    if not success:
        print("✗ Sorting failed!")

if __name__ == "__main__":
    main()
