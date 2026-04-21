#!/usr/bin/env python3

import argparse
import simulatools
import re
from pathlib import Path
import json

from rich import pretty
from rich.console import Console

filepath = Path(__file__)
current_dir = filepath.parent
filepath = current_dir.resolve()
conf_file = current_dir / 'conf.json'

with conf_file.open('r') as conf_file:
    local_conf = json.load(conf_file)
caffeine_root = local_conf['caffeine_root']
resources = local_conf['resources'] if local_conf['resources'] != '' else caffeine_root
TRACES_DIR = f'{resources}'
RESULTS_DIR = local_conf['results'] if local_conf['results'] != '' else './results/'

pretty.install()
console = Console()

def get_trace_name(input_file: Path):
    stem = input_file.stem.lower()

    if 'twitter' in stem:
        match = re.search(r'twitter(\d{2})', stem)
        if match:
            return f'twitter{match.group(1)}'
    elif 'ibm' in stem or 'trace' in stem:
        match = re.search(r'trace(\d{3})', stem)
        if match:
            return f'trace{match.group(1)}'
    elif 'metakv' in stem:
        match = re.search(r'metakv([24])', stem)
        if match:
            return f'metakv{match.group(1)}'

    return stem


def run_mock(fname: str, trace_name: str, cache_size: int, trace_folder: str, algorithm_name: str) -> None:
    output_filename = f'{algorithm_name}-{trace_name}'
    output_path = Path(RESULTS_DIR) / f'{output_filename}.csv'

    console.log(f'[bold #a98467]Running {algorithm_name} on trace: {trace_name}, size: {cache_size}')

    if output_path.exists():
        console.log(f'[yellow]Output file already exists, skipping: {output_path}')
        return

    Path(RESULTS_DIR).mkdir(parents=True, exist_ok=True)

    single_run_result = simulatools.single_run(
        'mock',
        trace_file=fname,
        trace_folder=trace_folder,
        trace_format='LATENCY_RESULT',
        size=cache_size,
        additional_settings={},
        name=f'mock-{trace_name}',
        save=False,
        verbose=False
    )

    if single_run_result is False:
        console.log(f'[bold red]Error running {algorithm_name} on {fname}: exiting')
        exit(1)
    else:
        single_run_result['Cache Size'] = cache_size
        single_run_result['Trace'] = trace_name
        single_run_result['Algorithm'] = algorithm_name

        single_run_result.to_csv(output_path, index=False)
        console.log(f"[bold #ffd166]Results saved to: {output_path}")
        console.log(f"[bold #ffd166]Hit Rate: {single_run_result['Hit Rate'].iloc[0]:.4f}")
        console.log(f"[bold #ffd166]Avg. Penalty: {int(single_run_result['Average Penalty'].iloc[0])}")


def main():
    parser = argparse.ArgumentParser(
        description='Run mock policy experiments on marked traces (traces with LHD/LRB predictions)'
    )

    parser.add_argument('--input', help='Trace filename (not full path)', required=True, type=str)
    parser.add_argument('--trace-name', help='Name of the trace (overrides auto-detection)', required=False, type=str)
    parser.add_argument('--cache-size', help='Cache size in entries (overrides default)', required=False, type=int)
    parser.add_argument('--trace-folder', help='Trace folder within resources directory', required=False, type=str, default='latency')
    parser.add_argument('--algorithm-name', help='Name to use for output files (default: mock)', required=False, type=str, default='mock')

    args = parser.parse_args()

    console.print(f'[bold]Running mock experiments with args:[/bold]\n{args}')

    input_filename = args.input
    trace_file_path = Path(resources) / args.trace_folder / input_filename

    if not trace_file_path.exists():
        console.print(f'[bold red]Error: Trace file does not exist: {trace_file_path}')
        exit(1)

    trace_name = args.trace_name if args.trace_name else get_trace_name(Path(input_filename))
    cache_size = 1024 # not relevant for mock

    console.print(f'[bold cyan]Trace name: {trace_name}')
    console.print(f'[bold cyan]Cache size: {cache_size}')
    console.print(f'[bold cyan]Trace folder: {args.trace_folder}')
    console.print(f'[bold cyan]Full trace path: {trace_file_path}')

    dump_path = Path(caffeine_root)
    console.print(f'[bold yellow]Cleaning up temporary CSV files in {dump_path}')

    for csv_file in dump_path.rglob('*.csv'):
        csv_file.unlink()
        console.print(f'[dim]Removed {csv_file.name}')

    run_mock(input_filename, trace_name, cache_size, args.trace_folder, args.algorithm_name)

    console.print(f'[bold green]Completed successfully!')


if __name__ == '__main__':
    main()
