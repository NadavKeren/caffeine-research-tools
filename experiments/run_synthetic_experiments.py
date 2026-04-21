#!/usr/bin/env python3

import argparse
import simulatools
import random
from pathlib import Path
import json
import pandas as pd
import numpy as np

from rich import pretty
from rich.console import Console
from rich.table import Table

from synthetic_trace_gen import gen_trace, TRACE_CONF
from split_synthetic_results import split_results_dump

filepath = Path(__file__)
current_dir = filepath.parent
filepath = current_dir.resolve()
conf_file = current_dir / 'conf.json'

with conf_file.open('r') as conf_file:
    local_conf = json.load(conf_file)
caffeine_root = Path(local_conf['caffeine_root'])
resources = local_conf['resources'] if local_conf['resources'] != '' else caffeine_root
TRACES_DIR = f'{resources}'
RESULTS_DIR = Path(local_conf['results']) if local_conf['results'] != '' else Path('./results/')

pretty.install()
console = Console()

NUM_OF_QUANTA = 16
TRACE_FOLDER = 'synthetic'

PIPELINE_CA_LRU_ONLY = {
    "pipeline.num-of-blocks": 1,
    "pipeline.num-of-quanta": 16,
    "pipeline.blocks.0.type": "LA-LRU",
    "pipeline.blocks.0.quota": 16,
    "pipeline.blocks.0.decay-factor": 1,
    "pipeline.blocks.0.max-lists": 10
}

PIPELINE_CA_LFU_ONLY = {
    "pipeline.num-of-blocks": 1,
    "pipeline.num-of-quanta": 16,
    "pipeline.blocks.0.type": "LA-LFU",
    "pipeline.blocks.0.quota": 16,
    "pipeline.blocks.0.decay-factor": 1,
    "pipeline.blocks.0.max-lists": 10
}

PIPELINE_LBU_ONLY = {
    "pipeline.num-of-blocks": 1,
    "pipeline.num-of-quanta": 16,
    "pipeline.blocks.0.type": "LBU",
    "pipeline.blocks.0.quota": 16,
    "pipeline.burst.aging-window-size": 50,
    "pipeline.burst.age-smoothing": 0.0025,
    "pipeline.burst.number-of-partitions": 4,
    "pipeline.burst.type": "normal",
    "pipeline.burst.sketch.eps": 0.0001,
    "pipeline.burst.sketch.confidence": 0.99
}


def run_pipeline_experiment(trace_file: str, cache_size: int, algorithm_name: str,
                           config: dict) -> Path:
    console.log(f'[bold #a98467]Running {algorithm_name} pipeline on synthetic trace, cache size: {cache_size}')

    quantum_size = cache_size / NUM_OF_QUANTA
    settings = {**config, 'pipeline.quantum-size': quantum_size}

    single_run_result = simulatools.single_run(
        'sampled_ghost',
        trace_file=trace_file,
        trace_folder=TRACE_FOLDER,
        trace_format='LATENCY',
        size=cache_size,
        additional_settings=settings,
        name=f'{algorithm_name}-synthetic',
        save=False,
        verbose=False
    )

    if single_run_result is False:
        console.log(f'[bold red]Error running {algorithm_name} pipeline: exiting')
        exit(1)

    console.log(f"[bold #ffd166]{algorithm_name} - Hit Rate: {single_run_result['Hit Rate'].iloc[0]:.4f}")
    console.log(f"[bold #ffd166]{algorithm_name} - Avg. Penalty: {int(single_run_result['Average Penalty'].iloc[0])}")

    dump_files = list(caffeine_root.rglob('*.results_dump'))
    if len(dump_files) != 1:
        console.log(f'[bold red]Error: Expected 1 results_dump file, found {len(dump_files)}')
        exit(1)

    dump_file = dump_files[0]
    destination = RESULTS_DIR / f'{algorithm_name}-synthetic.results_dump'
    dump_file.rename(destination)
    console.log(f'[bold green]Saved results dump to: {destination}')

    return destination


def run_mock_on_split(split_file: Path, cache_size: int, algorithm_name: str,
                     trace_type: str) -> dict:
    console.log(f'[bold #6c7e3a]Running mock for {algorithm_name} on {trace_type}')

    single_run_result = simulatools.single_run(
        'mock',
        trace_file=split_file.name,
        trace_folder=TRACE_FOLDER,
        trace_format='LATENCY_RESULT',
        size=cache_size,
        additional_settings={},
        name=f'mock-{algorithm_name}-{trace_type}',
        save=False,
        verbose=False
    )

    if single_run_result is False:
        console.log(f'[bold red]Error running mock on {split_file.name}')
        exit(1)

    hit_rate = single_run_result['Hit Rate'].iloc[0]
    avg_penalty = single_run_result['Average Penalty'].iloc[0]

    console.log(f'[bold #bedcfe]{algorithm_name}/{trace_type} - Hit Rate: {hit_rate:.4f}, Avg Penalty: {int(avg_penalty)}')

    return {
        'algorithm': algorithm_name,
        'trace_type': trace_type,
        'hit_rate': hit_rate,
        'avg_penalty': avg_penalty
    }


def generate_results_table(results: list) -> None:
    algorithms = ['LRU', 'LFU', 'LBU']
    trace_types = ['recency', 'frequency', 'burstiness']

    results_dict = {}
    for r in results:
        key = (r['algorithm'], r['trace_type'])
        results_dict[key] = r['avg_penalty']

    table = Table(title="Average Latency Results", show_header=True, header_style="bold magenta")
    table.add_column("Algorithm / Traffic", style="cyan", width=12)
    table.add_column("Recency", justify="right", style="green")
    table.add_column("Frequency", justify="right", style="green")
    table.add_column("Burstiness", justify="right", style="green")

    for algo in algorithms:
        row = [algo]
        for trace_type in trace_types:
            penalty = results_dict.get((algo, trace_type), 0)
            row.append(f"{int(penalty)}")
        table.add_row(*row)

    console.print("\n")
    console.print(table)
    console.print("\n")

    csv_data = []
    for algo in algorithms:
        for trace_type in trace_types:
            penalty = results_dict.get((algo, trace_type), 0)
            csv_data.append({
                'Algorithm': algo,
                'Traffic_Type': trace_type,
                'Average_Penalty': penalty
            })

    df = pd.DataFrame(csv_data)
    csv_path = RESULTS_DIR / 'synthetic_splitted_results.csv'
    df.to_csv(csv_path, index=False)
    console.print(f'[bold green]Results saved to: {csv_path}')


def main():
    parser = argparse.ArgumentParser(
        description='Run synthetic trace experiments with LRU/LFU/LBU pipelines'
    )

    parser.add_argument('--cache-size', help='Cache size in entries', required=True, type=int)
    parser.add_argument('--seed', help='Random seed for trace generation', type=int, required=False)
    parser.add_argument('--skip-trace-gen', help='Skip trace generation and use existing trace', action='store_true', required=False)

    args = parser.parse_args()

    console.print(f'[bold]Running synthetic experiments with args:[/bold]\n{args}')

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    trace_folder_path = Path(resources) / TRACE_FOLDER
    trace_folder_path.mkdir(parents=True, exist_ok=True)

    trace_file = 'synthetic.trace'
    target_trace = trace_folder_path / trace_file

    if args.skip_trace_gen:
        console.print(f'[bold yellow]Skipping trace generation, using existing trace at: {target_trace}')
        if not target_trace.exists():
            console.print(f'[bold red]Error: Trace file not found at {target_trace}')
            exit(1)
    else:
        seed = args.seed if args.seed is not None else random.randint(1_000_000, 9_999_999)
        console.print(f'[bold cyan]Generating synthetic trace with seed: {seed:,}')

        rng = np.random.default_rng(seed=seed)
        random.seed(seed)
        gen_trace(rng)

        source_trace = Path(trace_file)
        source_trace.rename(target_trace)
        console.print(f'[bold green]Synthetic trace generated and moved to: {target_trace}')

    console.print(f'[bold yellow]Cleaning up temporary files in {caffeine_root}')
    for csv_file in caffeine_root.rglob('*.csv'):
        csv_file.unlink()
    for dump_file in caffeine_root.rglob('*.results_dump'):
        dump_file.unlink()

    experiments = [
        ('LRU', PIPELINE_CA_LRU_ONLY),
        ('LFU', PIPELINE_CA_LFU_ONLY),
        ('LBU', PIPELINE_LBU_ONLY)
    ]

    console.print('\n[bold magenta]Running pipeline experiments...')
    dump_files = {}

    for algo_name, config in experiments:
        expected_dump = RESULTS_DIR / f'{algo_name}-synthetic.results_dump'

        if expected_dump.exists():
            console.print(f'[bold yellow]Skipping {algo_name} experiment, dump file already exists: {expected_dump}')
            dump_files[algo_name] = expected_dump
        else:
            dump_path = run_pipeline_experiment(
                trace_file,
                args.cache_size,
                algo_name,
                config
            )
            dump_files[algo_name] = dump_path

    console.print('\n[bold magenta]Splitting results dumps by traffic type...')

    split_files = {}
    for algo_name, dump_file in dump_files.items():
        console.print(f'\n[bold cyan]Splitting {algo_name} results...')
        split_output_dir = RESULTS_DIR / 'splits' / algo_name

        split_paths = split_results_dump(dump_file, split_output_dir)

        for traffic_type, split_file in split_paths.items():
            target_location = trace_folder_path / split_file.name
            split_file.rename(target_location)
            if algo_name not in split_files:
                split_files[algo_name] = {}
            split_files[algo_name][traffic_type] = target_location

    console.print('\n[bold magenta]Running mock experiments on split files...')

    all_results = []
    for algo_name in ['LRU', 'LFU', 'LBU']:
        for trace_type in ['recency', 'frequency', 'burstiness']:
            split_file = split_files[algo_name][trace_type]
            console.print(f'\n[bold cyan]Running mock for split: {split_file}')
            result = run_mock_on_split(
                split_file,
                args.cache_size,
                algo_name,
                trace_type
            )
            all_results.append(result)

    generate_results_table(all_results)

    console.print('[bold green]Done')


if __name__ == '__main__':
    main()
