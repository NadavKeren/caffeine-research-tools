from plot_pareto_front import create_pareto

import argparse
import simulatools
import json
import re
from pathlib import Path

from rich import print, pretty
from rich.progress import Progress
pretty.install()



conf_path = Path.cwd() / 'conf.json'
with conf_path.open('r') as conf_file:
    local_conf = json.load(conf_file)
caffeine_root = local_conf['caffeine_root']
resources = local_conf['resources'] if local_conf['resources'] != '' else caffeine_root
TRACES_DIR = f'{resources}'


PIPELINE_LRU_ONLY = {"pipeline.num-of-blocks" : 1, 
                     "pipeline.num-of-quanta" : 16,
                     "pipeline.blocks.0.type": "LRU",
                     "pipeline.blocks.0.quota": 16}

def generate_powers_of_two(low, high):
    n = 1 << low
    for i in range(low, high + 1):
        yield n
        n = n << 1 


def run_test(file: Path, trace_name: str, cache_size: int, 
             output_file: Path, progress_console) -> None:
        
    progress_console.log(f'[bold #a98467]Running LRU on trace: {trace_name}, size: {cache_size}')
    
    if (output_file.exists()): # * Skipping tests with existing results        
        return
    
    SIZE_SETTINGS = {'pipeline.quantum-size' : cache_size // 16}
    SETTINGS = {**PIPELINE_LRU_ONLY, **SIZE_SETTINGS}
    single_run_result = simulatools.single_run('pipeline', trace_files=[file.name], trace_folder='latency', 
                                                trace_format='LATENCY', size=cache_size,
                                                additional_settings=SETTINGS,
                                                name=f'LRU-{trace_name}-{cache_size}',
                                                save = False, verbose = False)
    
    if (single_run_result is False):
        progress_console.log(f'[bold red]Error in {file.name}: exiting')
        exit(1)
    else:                    
        single_run_result['Cache Size'] = cache_size
        single_run_result['Trace'] = trace_name
        
        single_run_result.to_csv(output_file.resolve())
        progress_console.log(f"[bold #ffd166]Avg. Pen. {int(single_run_result['Average Penalty'].iloc[0])}")
        
def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-i', '--input-path', help='The the path to the input file to check', type=str, required=True)
    parser.add_argument('-o' ,'--output-path', help='The the path to the output the results', type=str, required=True)
    
    args = parser.parse_args()
    input_file = Path(args.input_path)
    if not input_file.exists() or not input_file.is_file():
        print(f'[bold red]Error: bad input given: {args.input_path}')
        
    if input_file.stem.startswith("google-cluster"):
        trace_name = "google-cluster1"
    elif input_file.stem.startswith("twitter-cluster"):
        match = re.match(r'^(twitter-cluster\d+)', input_file.stem)
        trace_name = match.group(1)
    elif input_file.stem.startswith('metakv'):
        match = re.match(r'^(metakv\d+)', input_file.stem)
        trace_name = match.group(1)
        
    output_dir = Path(args.output_path) / trace_name
    output_dir.mkdir(exist_ok=True)
    
    print(f'[bold]Writing results to the directory: {output_dir.resolve()}')
    
    with Progress() as progress:
        progress.console.log(f'Starting {trace_name}')
        cache_size_progress =  progress.add_task('[bold #adc178]Size progress', total=11, start=True)
        for cache_size in generate_powers_of_two(7, 17):
            run_test(input_file, trace_name, cache_size, output_dir / f'{input_file.stem}-{cache_size}.csv', progress.console)
            progress.update(cache_size_progress, advance=1)
    
    create_pareto(output_dir)


if __name__ == "__main__":
    main()     
