from pathlib import Path
from typing import Dict
from rich import print
import json

config_file = Path(__file__).parent / 'synthetic_trace_config.json'
with config_file.open('r') as f:
    config = json.load(f)

TRACE_CONF = config['items']


def split_results_dump(input_file: Path, output_dir: Path = None) -> Dict[str, Path]:
    recency_start = 0
    recency_end = TRACE_CONF['recency']

    frequency_start = recency_end
    frequency_end = frequency_start + TRACE_CONF['frequency']

    burstiness_start = frequency_end
    burstiness_end = burstiness_start + TRACE_CONF['burstiness']

    print(f'[bold cyan]Key ranges:')
    print(f'  RECENCY: [{recency_start}, {recency_end})')
    print(f'  FREQUENCY: [{frequency_start}, {frequency_end})')
    print(f'  BURSTINESS: [{burstiness_start}, {burstiness_end})')

    if output_dir is None:
        output_dir = input_file.parent
    else:
        output_dir.mkdir(exist_ok=True, parents=True)

    print(f'[bold cyan]Splitting {input_file} into traffic types in {output_dir}')

    base_name = input_file.stem
    recency_output = output_dir / f'{base_name}_recency.results_dump'
    frequency_output = output_dir / f'{base_name}_frequency.results_dump'
    burstiness_output = output_dir / f'{base_name}_burstiness.results_dump'

    recency_count = 0
    frequency_count = 0
    burstiness_count = 0
    other_count = 0
    row_count = 0

    with input_file.open('r') as input_f, \
         recency_output.open('w') as recency_f, \
         frequency_output.open('w') as frequency_f, \
         burstiness_output.open('w') as burstiness_f:

        for line in input_f:
            row_count += 1
            parts = line.split()
            if len(parts) != 4:
                continue

            timestamp, key, miss_penalty, result = parts
            key = int(key)

            if recency_start <= key < recency_end:
                recency_f.write(line)
                recency_count += 1
            elif frequency_start <= key < frequency_end:
                frequency_f.write(line)
                frequency_count += 1
            elif burstiness_start <= key < burstiness_end:
                burstiness_f.write(line)
                burstiness_count += 1
            else:
                other_count += 1

    print(f'\n[bold green]Processing complete:')
    print(f'  RECENCY entries: {recency_count:,}')
    print(f'  FREQUENCY entries: {frequency_count:,}')
    print(f'  BURSTINESS entries: {burstiness_count:,}')
    print(f'  ONE HIT WONDERS entries (skipped): {other_count:,}')

    print(f'\n[bold cyan]Output files:')
    print(f'  {recency_output}')
    print(f'  {frequency_output}')
    print(f'  {burstiness_output}')

    return {
        'recency': recency_output,
        'frequency': frequency_output,
        'burstiness': burstiness_output
    }
