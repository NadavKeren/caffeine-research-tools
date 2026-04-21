import argparse
import simulatools
import re
from os import urandom
from pathlib import Path
import json
import shutil

from rich import pretty
from rich.console import Console
from rich.progress import Progress

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

OUTPUT_SUFFIX = ""

NUM_OF_QUANTA = 16

SIZES = {'ibm010' : 2 ** 9, 'ibm024' : 2 ** 9, 'ibm031' : 2 ** 16,
         'ibm045' : 2 ** 12, 'ibm034' : 2 ** 14, 'ibm029' : 2 ** 9,
         'ibm012' : 2 ** 10, 'twitter01' : 2 ** 10, 'twitter03' : 2 ** 10,
         'twitter09' : 2 ** 12, 'twitter28' : 2 ** 12, "metakv4" : 2 ** 13,
         "metakv2" : 2 ** 13}

PIPELINE_CA_SETTINGS_WITHOUT_QUOTA = {"pipeline.num-of-blocks" : 3,
                                      "pipeline.blocks.0.type": "LA-LRU",
                                      "pipeline.blocks.0.decay-factor" : 1, 
                                      "pipeline.blocks.0.max-lists" : 10,
                                      "pipeline.blocks.1.type": "LA-LFU",
                                      "pipeline.blocks.1.decay-factor" : 1, 
                                      "pipeline.blocks.1.max-lists" : 10,
                                      "pipeline.blocks.2.type": "LBU",
                                      "pipeline.burst.aging-window-size" : 50, 
                                      "pipeline.burst.age-smoothing" : 0.0025, 
                                      "pipeline.burst.number-of-partitions" : 4, 
                                      "pipeline.burst.type" : "normal"}

#* To the above config, we add the quotas per block

PIPELINE_EQUAL_START_SETTINGS = {**PIPELINE_CA_SETTINGS_WITHOUT_QUOTA,
                                 "pipeline.blocks.0.quota": 5, 
                                 "pipeline.blocks.1.quota": 6, 
                                 "pipeline.blocks.2.quota": 5}

PIPELINE_LRU_START_SETTINGS = {**PIPELINE_CA_SETTINGS_WITHOUT_QUOTA,
                               "pipeline.blocks.0.quota": 14, 
                               "pipeline.blocks.1.quota": 1,
                               "pipeline.blocks.2.quota": 1}

PIPELINE_SETTINGS_WITHOUT_BURST = {"pipeline.num-of-blocks" : 2,
                                   "pipeline.blocks.0.type": "LA-LRU",
                                   "pipeline.blocks.0.decay-factor" : 1, 
                                   "pipeline.blocks.0.max-lists" : 10,
                                   "pipeline.blocks.0.quota": 8, 
                                   "pipeline.blocks.1.type": "LA-LFU",
                                   "pipeline.blocks.1.decay-factor" : 1, 
                                   "pipeline.blocks.1.max-lists" : 10,
                                   "pipeline.blocks.1.quota": 8}

PIPELINE_CA_LRU_ONLY = {"pipeline.num-of-blocks" : 1, 
                        "pipeline.num-of-quanta" : 16,
                        "pipeline.blocks.0.type": "LA-LRU",
                        "pipeline.blocks.0.quota": 16, 
                        "pipeline.blocks.0.decay-factor" : 1, 
                        "pipeline.blocks.0.max-lists" : 10}

PIPELINE_CA_LFU_ONLY = {"pipeline.num-of-blocks" : 1, 
                        "pipeline.num-of-quanta" : 16,
                        "pipeline.blocks.0.type": "LA-LFU",
                        "pipeline.blocks.0.quota": 16, 
                        "pipeline.blocks.0.decay-factor" : 1, 
                        "pipeline.blocks.0.max-lists" : 10}


PIPELINE_LBU_ONLY = {"pipeline.num-of-blocks" : 1, 
                    "pipeline.num-of-quanta" : 16,
                    "pipeline.blocks.0.type": "LBU",
                    "pipeline.blocks.0.quota": 16, 
                    "pipeline.burst.aging-window-size" : 50, 
                    "pipeline.burst.age-smoothing" : 0.0025, 
                    "pipeline.burst.number-of-partitions" : 4, 
                    "pipeline.burst.type" : "normal", 
                    "pipeline.burst.sketch.eps" : 0.0001, 
                    "pipeline.burst.sketch.confidence" : 0.99}

FGHC_SETTINGS = {'sampled-hill-climber.sample-order-factor' : 0, 
                 'sampled-hill-climber.adaption-multiplier' : 10}

SEED_PATH = 'random-seed'


SETTINGS = {"pipeline.num-of-quanta" : NUM_OF_QUANTA,
            "pipeline.burst.aging-window-size" : 50, 
            "pipeline.burst.age-smoothing" : 0.0025, 
            "pipeline.burst.number-of-partitions" : 4, 
            "pipeline.burst.type" : "normal", 
            "pipeline.burst.sketch.eps" : 0.0001, 
            "pipeline.burst.sketch.confidence" : 0.99,
            'full-ghost-hill-climber.adaption-multiplier' : 10}


def get_trace_name(input_file: Path):
    if input_file.stem.startswith("twitter-cluster"):
        match = re.match(r'^(twitter-cluster\d+)', input_file.stem)
        trace_name = match.group(1)
    elif input_file.stem.startswith("ibm"):
        temp_fname = input_file.stem.lower()
        name = re.findall('ibm0[0-9][0-9]', temp_fname)
        trace_name = name[0]
    
    return trace_name


def get_dists(file: Path):
    filename = file.stem
    
    a_pos = filename.find('-A-')
    if a_pos == -1:
        return None

    return filename[a_pos:]   


def run_test(fname: str, trace_name: str, cache_size: int, output_filename : str,
             algorithm : str, should_keep_dump : bool = False, additional_settings = None,
             name = None, additional_csv_data = None, progress_console = None) -> None:
    print(output_filename)
    if progress_console:
        if name is None:
            progress_console.log(f'[bold #a98467]Running {algorithm} on trace: {trace_name}, size: {cache_size}' + f' Name: {name}' if name is not None else "")
        else:
            progress_console.log(f'[bold #a98467]Running {algorithm}: {name}, size: {cache_size}')
    else:
        console.log(f'[bold #a98467]Running {algorithm} on trace: {trace_name}, size: {cache_size}' + f' Name: {name}' if name is not None else "")
    
    if (Path(f'{RESULTS_DIR}/{output_filename}.csv').exists()): # * Skipping tests with existing results        
        return
    
    settings = SETTINGS if additional_settings is None else {**SETTINGS, **additional_settings}
        
    single_run_result = simulatools.single_run(algorithm, trace_file=fname, trace_folder='latency', 
                                                trace_format='LATENCY', size=cache_size,
                                                additional_settings=settings,
                                                name=f'{algorithm}-{trace_name}' if name is None else f'{algorithm}-{trace_name}-{name}',
                                                save = False, verbose = False)
    
    if (single_run_result is False):
        if progress_console:
            progress_console.log(f'[bold red]Error in {fname}: exiting')
        else:
            console.log(f'[bold red]Error in {fname}: exiting')
        
        exit(1)
    else:                    
        single_run_result['Cache Size'] = cache_size
        single_run_result['Trace'] = trace_name
        
        if additional_csv_data is not None:
            for key, value in additional_csv_data.items():
                single_run_result[key] = value
        
        
        single_run_result.to_csv(f'{RESULTS_DIR}/{output_filename}.csv')
        if progress_console:
            progress_console.log(f"[bold #ffd166]Avg. Pen. {int(single_run_result['Average Penalty'].iloc[0])}")
        else:
            console.log(f"[bold #ffd166]Avg. Pen. {int(single_run_result['Average Penalty'].iloc[0])}")
        
        if should_keep_dump:
            dump_path = Path(f'{caffeine_root}')

            quota_files = [file.resolve() for file in dump_path.rglob('*.quota_dump')]
            if len(quota_files) == 1:
                dumpfile = quota_files[0]
                destination = Path(RESULTS_DIR) / f'{output_filename}.quota_dump'
                shutil.move(dumpfile, destination)
            elif len(quota_files) > 1:
                if progress_console:
                    progress_console.log(f"[bold red]Wrong number of quota dump files found: {len(quota_files)}")
                else:
                    console.print(f"[bold red]Wrong number of quota dump files found: {len(quota_files)}")
                raise AssertionError()

            results_files = [file.resolve() for file in dump_path.rglob('*.results_dump')]
            if len(results_files) > 1:
                resultsfile = results_files[0]
                destination = Path(RESULTS_DIR) / f'{output_filename}.results_dump'
                shutil.move(resultsfile, destination)
            elif len(results_files) > 1:
                if progress_console:
                    progress_console.log(f"[bold red]Wrong number of results dump files found: {len(results_files)}")
                else:
                    console.print(f"[bold red]Wrong number of results dump files found: {len(results_files)}")
                raise AssertionError()

        #* Clean the results_dump files, relevant only to the synthetic trace experiments
        results_files = [file.resolve() for file in Path(f'{caffeine_root}').rglob('*.results_dump')]
        for file in results_files:
                file.unlink()


def run_full_ghost(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
    
    csv_filename = f'FGHC-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'sampled_ghost', 
                name='FGHC', additional_settings={**PIPELINE_EQUAL_START_SETTINGS, **FGHC_SETTINGS,
                                                  **SIZE_SETTINGS},
                should_keep_dump=True)
    
    csv_filename = f'FGHC-RF-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'sampled_ghost', 
                name='FGHC-RF', additional_settings={**PIPELINE_SETTINGS_WITHOUT_BURST, **FGHC_SETTINGS,
                                                     **SIZE_SETTINGS},
                should_keep_dump=True)


def run_sampled_all(fname: str, trace_name: str, cache_size: int, round: int, seed: int, progress: Progress) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]

    sample_progress = progress.add_task('[bold #bedcfe]Current round', total=6, start=True)
    for sample_rate in range(1, 7):
        if (int(quantum_size) >> sample_rate > 0):
            SAMPLE_SETTINGS = {'sampled-hill-climber.sample-order-factor' : sample_rate, 
                            'sampled-hill-climber.adaption-multiplier' : 10}
            
            SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
            
            csv_filename = f'sampled-O{sample_rate}-{OUTPUT_SUFFIX}-R{round}'
            run_test(fname, trace_name, cache_size, csv_filename, 'sampled_ghost', 
                    name=f'O{sample_rate}', additional_settings={**PIPELINE_EQUAL_START_SETTINGS, 
                                                                        **SAMPLE_SETTINGS, 
                                                                        **SIZE_SETTINGS, 
                                                                        SEED_PATH: seed},
                    additional_csv_data={'Round' : round, 'Seed': seed}, progress_console=progress.console, should_keep_dump=True)
        progress.update(sample_progress, advance=1)
    progress.remove_task(sample_progress)


def run_single_sampled(fname: str, trace_name: str, cache_size: int, round: int, seed: int, progress: Progress, sample_rate: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]

    SAMPLE_SETTINGS = {'sampled-hill-climber.sample-order-factor' : sample_rate, 
                       'sampled-hill-climber.adaption-multiplier' : 10}
    
    SIZE_SETTINGS = {'pipeline.quantum-size' : quantum_size}
    
    csv_filename = f'sampled-O{sample_rate}-{OUTPUT_SUFFIX}-R{round}'
    run_test(fname, trace_name, cache_size, csv_filename, 'sampled_ghost', 
            name=f'O{sample_rate}', additional_settings={**PIPELINE_EQUAL_START_SETTINGS, 
                                                                **SAMPLE_SETTINGS, 
                                                                **SIZE_SETTINGS, 
                                                                SEED_PATH: seed},
            additional_csv_data={'Round' : round, 'Seed': seed}, progress_console=progress.console, should_keep_dump=True)


def run_all_simple(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}
    
    csv_filename = f'LRU-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'pipeline', 
            name='LRU', additional_settings={**PIPELINE_CA_LRU_ONLY, **SIZE_SETTINGS}, should_keep_dump=False)

    csv_filename = f'LFU-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'pipeline', 
            name='LFU', additional_settings={**PIPELINE_CA_LFU_ONLY, **SIZE_SETTINGS}, should_keep_dump=False)
    
    csv_filename = f'LBU-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'pipeline', 
            name='LBU', additional_settings={**PIPELINE_LBU_ONLY, **SIZE_SETTINGS}, should_keep_dump=False)


def run_grid_search(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}
    
    with Progress() as progress:
        lru_progress = progress.add_task('[bold #adc178]LA-LRU quota', total=16, start=True)
        lfu_progress = progress.add_task('[bold #bedcfe]LA-LFU quota', total=16, start=True)
        for lru_size in range(NUM_OF_QUANTA + 1):
            for lfu_size in range(NUM_OF_QUANTA - lru_size + 1):
                bc_size = NUM_OF_QUANTA - (lru_size + lfu_size)
                csv_filename = f'static-{lru_size}-{lfu_size}-{bc_size}-{OUTPUT_SUFFIX}'
                run_test(fname, trace_name, cache_size, csv_filename, 'pipeline',
                         name=f"{lru_size}-{lfu_size}-{bc_size}", 
                         additional_settings={**PIPELINE_CA_SETTINGS_WITHOUT_QUOTA,
                                              "pipeline.blocks.0.quota": lru_size, 
                                              "pipeline.blocks.1.quota": lfu_size,
                                              "pipeline.blocks.2.quota": bc_size,
                                              **SIZE_SETTINGS},
                         should_keep_dump=False,
                         additional_csv_data={'LRU Size': lru_size, 'LFU Size': lfu_size, 'LBU Size': bc_size},
                         progress_console=progress.console)
                progress.update(lfu_progress, advance=1)
            
            progress.update(lru_progress, advance=1)
            progress.reset(lfu_progress, total=(NUM_OF_QUANTA - lru_size - 1))

                  
def run_adaptive_CA(fname: str, trace_name: str, cache_size: int) -> None:
    csv_filename = f'ACA-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'adaptive_ca',
             should_keep_dump=False)
    

def run_other(fname: str, trace_name: str, cache_size: int):
    csv_filename = f'Hyperbolic-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'hyperbolic', name="hyperbolic", should_keep_dump=False)
    
    csv_filename = f'GDWheel-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'gdwheel', name="GD-Wheel", should_keep_dump=False)
    
    csv_filename = f'ARC-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'arc', name="ARC", should_keep_dump=False)
    
    csv_filename = f'FRD-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'frd', name="FRD", should_keep_dump=False)
    
    csv_filename = f'LA-Cache-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'yan_li', name="LA-Cache", should_keep_dump=False)

    csv_filename = f'S3-FIFO-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 's3_fifo', name="S3-FIFO", should_keep_dump=False)

    csv_filename = f'SIEVE-{OUTPUT_SUFFIX}'
    run_test(fname, trace_name, cache_size, csv_filename, 'sieve', name="SIEVE", should_keep_dump=False)
    
    
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--input', help="The input trace path", required=True)
    parser.add_argument('--trace-name', help="The name of the trace, default is reading from the file-name", required=False, type=str)
    parser.add_argument('--rounds', help="number of round to perform", required=False, type=int)
    parser.add_argument('--round-index-start', help="The starting index for the round numbers", required=False, type=int, default=0)
    parser.add_argument('--cache-size', help="The cache size, overrides the default values", required=False, type=int)
    parser.add_argument('--run-all-shc', help="Run rounds of Sample Hill Climber with variable rates", action='store_true', required=False)
    parser.add_argument('--run-single-shc', help="Run rounds of Sample Hill Climber with a single rate", action='store_true', required=False)
    parser.add_argument('--run-aca', help="Run rounds of the Adaptive Cost-Aware Window-TinyLFU", action='store_true', required=False)
    parser.add_argument('--run-base', help="Run the baseline test of FGHC RFB and RF", action='store_true', required=False)
    parser.add_argument('--run-grid-search', help="Run grid search for finding the optimal static configuration", action='store_true', required=False)
    parser.add_argument('--run-other', help="Run comparison algorithms, not including LHD and LRB", action='store_true', required=False)

    args = parser.parse_args()

    console.print(f'[bold]Running with args:[/bold]\n{args}')

    dump_path = Path(caffeine_root)
    console.print(f'[bold yellow]Cleaning up temporary files in {dump_path}')

    for csv_file in dump_path.rglob('*.csv'):
        csv_file.unlink()
        console.print(f'[dim]Removed {csv_file.name}')

    for quota_file in dump_path.rglob('*.quota_dump'):
        quota_file.unlink()
        console.print(f'[dim]Removed {quota_file.name}')

    for results_file in dump_path.rglob('*.results_dump'):
        results_file.unlink()
        console.print(f'[dim]Removed {results_file.name}')
    
    file = Path(args.input)

    trace_name = args.trace_name if args.trace_name else file.stem.split('-')[0].lower()
    cache_size = args.cache_size if args.cache_size else SIZES.get(trace_name)
    dists = get_dists(file)

    if (cache_size is None):
        console.print(f'[bold red]Error: no default cache size for trace: {trace_name}, please provide a cache size using --cache-size')
        exit(1)
    
    global OUTPUT_SUFFIX
    OUTPUT_SUFFIX = f'{trace_name}-{dists}-{cache_size}'
    
    print(f'the output suffix will be: {OUTPUT_SUFFIX}')
    
    if args.run_base:
        run_full_ghost(file.name, trace_name, cache_size)
        run_all_simple(file.name, trace_name, cache_size)
        
    if args.rounds is not None and (args.run_single_shc is not None or args.run_all_shc):
        with Progress() as progress:
            round_progress = progress.add_task('[bold #adc178]Rounds', total=args.rounds, start=True)
            for round in range(args.rounds):
                seed = abs(int.from_bytes(urandom(4), 'big', signed=True))
                progress.console.log(f"Starting round {round + 1} of {args.rounds}: {100.0 * round / args.rounds}%, seed: {seed}",
                                     style='bold #adc178')
                
                if args.run_single_shc:
                    run_single_sampled(file.name, trace_name, cache_size, 
                                       args.round_index_start + round + 1, seed, 
                                       progress=progress, sample_rate=2)
                elif args.run_all_shc:
                    run_sampled_all(file.name, trace_name, cache_size, 
                                    args.round_index_start + round + 1, seed, 
                                    progress=progress)
                else:
                    raise AssertionError("Should be either run-single or run-all")
                
                progress.update(round_progress, advance=1)
    
    if args.run_aca:
        run_adaptive_CA(file.name, trace_name, cache_size)
    
    if args.run_grid_search:
        run_grid_search(file.name, trace_name, cache_size)
                
    if args.run_other:
        run_other(file.name, trace_name, cache_size)
        
    console.log("[bold #a3b18a]#####################\tDone\t#####################\n\n")


if __name__ == "__main__":
    main()
