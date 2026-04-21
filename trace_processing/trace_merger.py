import argparse
import re

from rich import pretty
from rich.console import Console
from rich.progress import Progress

from pathlib import Path
from os import SEEK_END

from typing import List

from itertools import islice

CONSOLE = Console()
pretty.install()


class TraceAction(argparse.Action):
    """Custom action to append trace files to a list."""
    def __call__(self, parser, namespace, values, option_string=None):
        if not hasattr(namespace, 'trace_list'):
            namespace.trace_list = []
        namespace.trace_list.append([values, 1])  # [trace_name, times]


class TimesAction(argparse.Action):
    """Custom action to set repetition count for the most recent trace."""
    def __call__(self, parser, namespace, values, option_string=None):
        if not hasattr(namespace, 'trace_list') or not namespace.trace_list:
            parser.error("--times must follow a --trace argument")
        namespace.trace_list[-1][1] = values  # Modify the times for the last trace


def get_trace_name(fname: str):
    temp_fname = fname.lower()
    name = re.findall('ibm0[0-9][0-9]', temp_fname)
    
    return name[0]


def read_last_line(file_path: Path):
    path = Path(file_path)
    with path.open('rb') as file:
        file.seek(0, SEEK_END)
        pos = file.tell() - 2
        while pos >= 0:
            file.seek(pos)
            if file.read(1) == b'\n':
                break
            pos -= 1
        last_line = file.readline().decode()
    return last_line


def calculate_file_start_and_end_times(file_path: Path) -> (int, int):
    with file_path.open('r') as file:
        first_line = file.readline()
    
    last_line = read_last_line(file_path) # more efficient for larger files
    
    start_time = first_line.split(' ')[0]
    end_time = last_line.split(' ')[0]
    
    return int(start_time), int(end_time)


def changeTimestampsAndWriteToFile(input_files: List[Path], output_path: Path):
    last_file_end = 0
    timestamp = 1
    file_ends = list()
    
    with output_path.open('w') as outputFile, Progress() as progress:
        file_progress = progress.add_task('[bold #bedcfe]Files added', total=len(input_files), start=True)
        for input_file in input_files:
            with input_file.open('r') as file_reader:
                num_of_lines = 0
                BATCH_SIZE = 10000
                lines = list(islice(file_reader, BATCH_SIZE))
                
                file_start, file_end = calculate_file_start_and_end_times(input_file)
                progress.console.print((file_start, file_end))
                
                while (lines) :
                    for line in lines:
                        written_time, key, miss_penalty = line.split(' ')
                        written_time = int(written_time)
                        key = int(key)
                        miss_penalty = float(miss_penalty.strip(' \n'))
                            
                        timestamp = written_time - file_start + last_file_end + 1
                        num_of_lines += 1
                            
                            
                        outputFile.write(f'{timestamp} {key} {miss_penalty}\n')
                        
                    lines = list(islice(file_reader, BATCH_SIZE))
                
                last_file_end = last_file_end + file_end - file_start
                file_ends.append((str(input_file), last_file_end, num_of_lines))
                
                progress.update(file_progress, advance=1)
                
    CONSOLE.print(file_ends)
    

def main():
    parser = argparse.ArgumentParser(
        description='Merge multiple IBM Object-Storage trace files with optional repetitions.',
        epilog='''
Example:
  python trace_merger.py --input-dir ./traces --trace traceA --trace traceB --times 5 --trace traceC

  This will merge traceA once, traceB five times, and traceC once.
  Output filename: traceA-traceBx5-traceC.trace

  If a trace appears only once, it's just the trace name.
  If a trace is repeated multiple times, it's formatted as: traceName + "x" + count
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--input-dir', help='The directory containing the original trace', type=str, required=True)
    parser.add_argument('--trace', help='A trace file to merge', type=str, action=TraceAction)
    parser.add_argument('--times', help='Number of times to repeat the previous trace (default: 1)', type=int, action=TimesAction)

    args = parser.parse_args()

    # Get the trace list from the namespace
    trace_list = getattr(args, 'trace_list', [])

    if not trace_list:
        parser.error("At least one --trace must be specified")

    input_dir = Path(args.input_dir)
    output_dir = input_dir / "merged"
    output_dir.mkdir(exist_ok=True)

    print(f'Input dir: {str(input_dir.resolve())} Output dir: {str(output_dir.resolve())}')

    # Expand traces based on repetition counts
    expanded_traces = []
    for trace, times in trace_list:
        expanded_traces.extend([trace] * times)

    input_files = [input_dir / trace for trace in expanded_traces]

    CONSOLE.print(f"[bold cyan]Trace configuration:[/]")
    for trace, times in trace_list:
        trace_name = get_trace_name(trace)
        CONSOLE.print(f"  {trace_name}: {times}x")
    CONSOLE.print(f"\n[bold cyan]Total files to merge:[/] {len(input_files)}")

    for file in input_files:
        start_time, end_time = calculate_file_start_and_end_times(file)
        CONSOLE.print(f'{str(file)}: {end_time - start_time}')

    filename_parts = []
    for trace, times in trace_list:
        trace_name = get_trace_name(trace)
        if times == 1:
            filename_parts.append(trace_name)
        else:
            filename_parts.append(f"{trace_name}x{times}")

    setname = "-".join(filename_parts) + ".trace"
    output_path = output_dir / setname
    changeTimestampsAndWriteToFile(input_files, output_path)
    
    CONSOLE.log("[bold #a3b18a]Done\n#####################\n\n")

                    
if __name__ == '__main__':
    main()
