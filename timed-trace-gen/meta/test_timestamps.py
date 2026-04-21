#!/usr/bin/env python3

import argparse

def validate_timestamps(filename):
    """
    Validate that timestamps in a file are non-decreasing.
    Reads line by line to handle large files efficiently.
    
    Args:
        filename: Path to the input file
        
    Returns:
        bool: True if all timestamps are non-decreasing, False otherwise
    """
    try:
        with open(filename, 'r') as file:
            prev_timestamp = None
            line_number = 0
            
            for line in file:
                line_number += 1
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Split line into timestamp and key
                parts = line.split(' ', 1)
                if len(parts) < 2:
                    print(f"Warning: Line {line_number} has invalid format: {line}")
                    continue
                
                try:
                    # Try to parse timestamp as float first, then as int
                    timestamp_str = parts[0]
                    try:
                        current_timestamp = float(timestamp_str)
                    except ValueError:
                        current_timestamp = int(timestamp_str)
                        
                except ValueError:
                    print(f"Error: Line {line_number} has invalid timestamp: {timestamp_str}")
                    return False
                
                # Check if timestamp is non-decreasing
                if prev_timestamp is not None and current_timestamp < prev_timestamp:
                    print(f"Error: Timestamp decreases at line {line_number}")
                    print(f"  Previous: {prev_timestamp}")
                    print(f"  Current:  {current_timestamp}")
                    return False
                
                prev_timestamp = current_timestamp
                
                # Progress indicator for large files
                if line_number % 1000000 == 0:
                    print(f"Processed {line_number:,} lines...")
    
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return False
    except IOError as e:
        print(f"Error reading file: {e}")
        return False
    
    print(f"Validation complete. Processed {line_number:,} lines.")
    return True

def main():
    parser = argparse.ArgumentParser(description='Validate that timestamps in a file are non-decreasing')
    parser.add_argument('filename', help='Input file to validate')
    
    args = parser.parse_args()
    
    print(f"Validating timestamps in: {args.filename}")
    
    if validate_timestamps(args.filename):
        print("✓ All timestamps are non-decreasing!")
    else:
        print("✗ Timestamp validation failed!")

if __name__ == "__main__":
    main()
