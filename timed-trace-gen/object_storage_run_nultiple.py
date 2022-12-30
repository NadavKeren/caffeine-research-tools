from subprocess import Popen
import argparse

def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d', '--dir', help="The trace directory within the origin dir")
    
    args = parser.parse_args()
    
    processes = [Popen(['python', 
                        './object_storage_delayed_hits_checker.py', 
                        f'-w {window_size}', 
                        '-b 1000000', 
                        f'-d {args.dir.strip(" ")}'])
                  
                 for window_size in (100, 1000, 10000)]
    
    exit_codes = [p.wait() for p in processes]
    
    
if __name__ == '__main__':
    main()