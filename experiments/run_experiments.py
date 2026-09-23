import argparse
import simulatools
import re
from itertools import permutations
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

#* The Exploring SHC (ESHC): the SHC plus one extra ghost cache placed at a random
#* configuration at least min-swap-distance quantum swaps away, re-drawn every
#* warmup-timeframes + evaluation-timeframes timeframes. max-rounds of -1 means unlimited.
ESHC_SETTINGS = {'exploring-sampled-hill-climber.sample-order-factor' : 0,
                 'exploring-sampled-hill-climber.adaption-multiplier' : 10,
                 'exploring-sampled-hill-climber.warmup-timeframes' : 2,
                 'exploring-sampled-hill-climber.evaluation-timeframes' : 8,
                 'exploring-sampled-hill-climber.min-swap-distance' : 6,
                 'exploring-sampled-hill-climber.max-rounds' : -1}

SBC_SETTINGS = {'statistics-based-climber.adaption-multiplier' : 10}

SEED_PATH = 'random-seed'

BLOCK_EXTRA_SETTINGS = {
    "LA-LRU": {"decay-factor": 1, "max-lists": 10},
    "LA-LFU": {"decay-factor": 1, "max-lists": 10},
    "LBU": {},
}
BLOCK_SHORT_NAMES = {"LA-LRU": "LRU", "LA-LFU": "LFU", "LBU": "LBU"}


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
             name = None, additional_csv_data = None, progress_console = None,
             force: bool = False) -> None:
    print(output_filename)
    if progress_console:
        if name is None:
            progress_console.log(f'[bold #a98467]Running {algorithm} on trace: {trace_name}, size: {cache_size}' + f' Name: {name}' if name is not None else "")
        else:
            progress_console.log(f'[bold #a98467]Running {algorithm}: {name}, size: {cache_size}')
    else:
        console.log(f'[bold #a98467]Running {algorithm} on trace: {trace_name}, size: {cache_size}' + f' Name: {name}' if name is not None else "")
    
    if (not force and Path(f'{RESULTS_DIR}/{output_filename}.csv').exists()): # * Skipping tests with existing results
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

            penalty_files = [file.resolve() for file in dump_path.rglob('*.avg_penalty_dump')]
            if len(penalty_files) == 1:
                penaltyfile = penalty_files[0]
                destination = Path(RESULTS_DIR) / f'{output_filename}.avg_penalty_dump'
                shutil.move(penaltyfile, destination)
            elif len(penalty_files) > 1:
                if progress_console:
                    progress_console.log(f"[bold red]Wrong number of avg penalty dump files found: {len(penalty_files)}")
                else:
                    console.print(f"[bold red]Wrong number of avg penalty dump files found: {len(penalty_files)}")
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
                         should_keep_dump=True,
                         additional_csv_data={'LRU Size': lru_size, 'LFU Size': lfu_size, 'LBU Size': bc_size},
                         progress_console=progress.console)
                progress.update(lfu_progress, advance=1)
            
            progress.update(lru_progress, advance=1)
            progress.reset(lfu_progress, total=(NUM_OF_QUANTA - lru_size - 1))

                  
def run_adaptive_grid_search(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}

    with Progress() as progress:
        for block_order in permutations(["LA-LRU", "LA-LFU", "LBU"]):
            order_label = "-".join(BLOCK_SHORT_NAMES[t] for t in block_order)
            first_type, second_type, third_type = block_order

            order_base_settings = {"pipeline.num-of-blocks": 3}
            for i, btype in enumerate(block_order):
                order_base_settings[f"pipeline.blocks.{i}.type"] = btype
                for k, v in BLOCK_EXTRA_SETTINGS[btype].items():
                    order_base_settings[f"pipeline.blocks.{i}.{k}"] = v

            first_block_progress = progress.add_task(f'[bold #adc178]{order_label} {BLOCK_SHORT_NAMES[first_type]} start quota', total=NUM_OF_QUANTA, start=True)
            second_block_progress = progress.add_task(f'[bold #bedcfe]{order_label} {BLOCK_SHORT_NAMES[second_type]} start quota', total=NUM_OF_QUANTA, start=True)

            for first_quota in range(NUM_OF_QUANTA + 1):
                for second_quota in range(NUM_OF_QUANTA - first_quota + 1):
                    third_quota = NUM_OF_QUANTA - (first_quota + second_quota)
                    csv_filename = f'FGHC-start-{order_label}-{first_quota}-{second_quota}-{third_quota}-{OUTPUT_SUFFIX}'
                    quota_settings = {
                        "pipeline.blocks.0.quota": first_quota,
                        "pipeline.blocks.1.quota": second_quota,
                        "pipeline.blocks.2.quota": third_quota,
                    }
                    csv_data = {
                        BLOCK_SHORT_NAMES[first_type] + ' Start': first_quota,
                        BLOCK_SHORT_NAMES[second_type] + ' Start': second_quota,
                        BLOCK_SHORT_NAMES[third_type] + ' Start': third_quota,
                        'Order': order_label,
                    }
                    run_test(fname, trace_name, cache_size, csv_filename, 'sampled_ghost',
                             name=f"start-{order_label}-{first_quota}-{second_quota}-{third_quota}",
                             additional_settings={**order_base_settings, **FGHC_SETTINGS,
                                                  **quota_settings, **SIZE_SETTINGS},
                             should_keep_dump=True,
                             additional_csv_data=csv_data,
                             progress_console=progress.console)
                    progress.update(second_block_progress, advance=1)

                progress.update(first_block_progress, advance=1)
                progress.reset(second_block_progress, total=(NUM_OF_QUANTA - first_quota - 1))

            progress.remove_task(first_block_progress)
            progress.remove_task(second_block_progress)


def run_exploring_grid_search(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}

    with Progress() as progress:
        for block_order in permutations(["LA-LRU", "LA-LFU", "LBU"]):
            order_label = "-".join(BLOCK_SHORT_NAMES[t] for t in block_order)
            first_type, second_type, third_type = block_order

            order_base_settings = {"pipeline.num-of-blocks": 3}
            for i, btype in enumerate(block_order):
                order_base_settings[f"pipeline.blocks.{i}.type"] = btype
                for k, v in BLOCK_EXTRA_SETTINGS[btype].items():
                    order_base_settings[f"pipeline.blocks.{i}.{k}"] = v

            first_block_progress = progress.add_task(f'[bold #adc178]{order_label} {BLOCK_SHORT_NAMES[first_type]} start quota', total=NUM_OF_QUANTA, start=True)
            second_block_progress = progress.add_task(f'[bold #bedcfe]{order_label} {BLOCK_SHORT_NAMES[second_type]} start quota', total=NUM_OF_QUANTA, start=True)

            for first_quota in range(NUM_OF_QUANTA + 1):
                for second_quota in range(NUM_OF_QUANTA - first_quota + 1):
                    third_quota = NUM_OF_QUANTA - (first_quota + second_quota)
                    csv_filename = f'ESHC-start-{order_label}-{first_quota}-{second_quota}-{third_quota}-{OUTPUT_SUFFIX}'
                    quota_settings = {
                        "pipeline.blocks.0.quota": first_quota,
                        "pipeline.blocks.1.quota": second_quota,
                        "pipeline.blocks.2.quota": third_quota,
                    }
                    csv_data = {
                        BLOCK_SHORT_NAMES[first_type] + ' Start': first_quota,
                        BLOCK_SHORT_NAMES[second_type] + ' Start': second_quota,
                        BLOCK_SHORT_NAMES[third_type] + ' Start': third_quota,
                        'Order': order_label,
                    }
                    run_test(fname, trace_name, cache_size, csv_filename, 'exploring_ghost',
                             name=f"start-{order_label}-{first_quota}-{second_quota}-{third_quota}",
                             additional_settings={**order_base_settings, **ESHC_SETTINGS,
                                                  **quota_settings, **SIZE_SETTINGS},
                             should_keep_dump=True,
                             additional_csv_data=csv_data,
                             progress_console=progress.console)
                    progress.update(second_block_progress, advance=1)

                progress.update(first_block_progress, advance=1)
                progress.reset(second_block_progress, total=(NUM_OF_QUANTA - first_quota - 1))

            progress.remove_task(first_block_progress)
            progress.remove_task(second_block_progress)

def run_statistics_grid_search(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}

    with Progress() as progress:
        for block_order in permutations(["LA-LRU", "LA-LFU", "LBU"]):
            order_label = "-".join(BLOCK_SHORT_NAMES[t] for t in block_order)
            first_type, second_type, third_type = block_order

            order_base_settings = {"pipeline.num-of-blocks": 3}
            for i, btype in enumerate(block_order):
                order_base_settings[f"pipeline.blocks.{i}.type"] = btype
                for k, v in BLOCK_EXTRA_SETTINGS[btype].items():
                    order_base_settings[f"pipeline.blocks.{i}.{k}"] = v

            # The SBC needs every block to hold at least a single quantum, an empty block has no
            # statistics to be scored by, thus each block starts with a quota of one or more.
            first_block_progress = progress.add_task(f'[bold #adc178]{order_label} {BLOCK_SHORT_NAMES[first_type]} start quota', total=(NUM_OF_QUANTA - 2), start=True)
            second_block_progress = progress.add_task(f'[bold #bedcfe]{order_label} {BLOCK_SHORT_NAMES[second_type]} start quota', total=(NUM_OF_QUANTA - 2), start=True)

            for first_quota in range(1, NUM_OF_QUANTA - 1):
                progress.reset(second_block_progress, total=(NUM_OF_QUANTA - first_quota - 1))

                for second_quota in range(1, NUM_OF_QUANTA - first_quota):
                    third_quota = NUM_OF_QUANTA - (first_quota + second_quota)
                    csv_filename = f'SBC-start-{order_label}-{first_quota}-{second_quota}-{third_quota}-{OUTPUT_SUFFIX}'
                    quota_settings = {
                        "pipeline.blocks.0.quota": first_quota,
                        "pipeline.blocks.1.quota": second_quota,
                        "pipeline.blocks.2.quota": third_quota,
                    }
                    csv_data = {
                        BLOCK_SHORT_NAMES[first_type] + ' Start': first_quota,
                        BLOCK_SHORT_NAMES[second_type] + ' Start': second_quota,
                        BLOCK_SHORT_NAMES[third_type] + ' Start': third_quota,
                        'Order': order_label,
                    }
                    run_test(fname, trace_name, cache_size, csv_filename, 'statistics_based',
                             name=f"start-{order_label}-{first_quota}-{second_quota}-{third_quota}",
                             additional_settings={**order_base_settings, **SBC_SETTINGS,
                                                  **quota_settings, **SIZE_SETTINGS},
                             should_keep_dump=True,
                             additional_csv_data=csv_data,
                             progress_console=progress.console)
                    progress.update(second_block_progress, advance=1)

                progress.update(first_block_progress, advance=1)

            progress.remove_task(first_block_progress)
            progress.remove_task(second_block_progress)


def run_reorder_grid_search(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}

    with Progress() as progress:
        for block_order in permutations(["LA-LRU", "LA-LFU", "LBU"]):
            order_label = "-".join(BLOCK_SHORT_NAMES[t] for t in block_order)
            first_type, second_type, third_type = block_order

            order_base_settings = {"pipeline.num-of-blocks": 3}
            for i, btype in enumerate(block_order):
                order_base_settings[f"pipeline.blocks.{i}.type"] = btype
                for k, v in BLOCK_EXTRA_SETTINGS[btype].items():
                    order_base_settings[f"pipeline.blocks.{i}.{k}"] = v

            first_block_progress = progress.add_task(f'[bold #adc178]{order_label} {BLOCK_SHORT_NAMES[first_type]} quota', total=NUM_OF_QUANTA, start=True)
            second_block_progress = progress.add_task(f'[bold #bedcfe]{order_label} {BLOCK_SHORT_NAMES[second_type]} quota', total=NUM_OF_QUANTA, start=True)

            for first_quota in range(NUM_OF_QUANTA + 1):
                for second_quota in range(NUM_OF_QUANTA - first_quota + 1):
                    third_quota = NUM_OF_QUANTA - (first_quota + second_quota)
                    csv_filename = f'static-{order_label}-{first_quota}-{second_quota}-{third_quota}-{OUTPUT_SUFFIX}'
                    quota_settings = {
                        "pipeline.blocks.0.quota": first_quota,
                        "pipeline.blocks.1.quota": second_quota,
                        "pipeline.blocks.2.quota": third_quota,
                    }
                    csv_data = {
                        BLOCK_SHORT_NAMES[first_type] + ' Size': first_quota,
                        BLOCK_SHORT_NAMES[second_type] + ' Size': second_quota,
                        BLOCK_SHORT_NAMES[third_type] + ' Size': third_quota,
                        'Order': order_label,
                    }
                    run_test(fname, trace_name, cache_size, csv_filename, 'pipeline',
                             name=f"{order_label}-{first_quota}-{second_quota}-{third_quota}",
                             additional_settings={**order_base_settings, **quota_settings, **SIZE_SETTINGS},
                             should_keep_dump=True,
                             additional_csv_data=csv_data,
                             progress_console=progress.console)
                    progress.update(second_block_progress, advance=1)

                progress.update(first_block_progress, advance=1)
                progress.reset(second_block_progress, total=(NUM_OF_QUANTA - first_quota - 1))

            progress.remove_task(first_block_progress)
            progress.remove_task(second_block_progress)


def run_adaptive_pipeline_reordered(fname: str, trace_name: str, cache_size: int) -> None:
    quantum_size = cache_size / SETTINGS["pipeline.num-of-quanta"]
    SIZE_SETTINGS = {'pipeline.quantum-size': quantum_size}

    # Equal start quotas matching PIPELINE_EQUAL_START_SETTINGS distribution
    EQUAL_QUOTAS = [5, 6, 5]

    with Progress() as progress:
        order_progress = progress.add_task('[bold #adc178]Block orderings', total=6, start=True)

        for block_order in permutations(["LA-LRU", "LA-LFU", "LBU"]):
            order_label = "-".join(BLOCK_SHORT_NAMES[t] for t in block_order)

            order_settings = {"pipeline.num-of-blocks": 3}
            for i, btype in enumerate(block_order):
                order_settings[f"pipeline.blocks.{i}.type"] = btype
                for k, v in BLOCK_EXTRA_SETTINGS[btype].items():
                    order_settings[f"pipeline.blocks.{i}.{k}"] = v
                order_settings[f"pipeline.blocks.{i}.quota"] = EQUAL_QUOTAS[i]

            csv_filename = f'FGHC-reordered-{order_label}-{OUTPUT_SUFFIX}'
            run_test(fname, trace_name, cache_size, csv_filename, 'sampled_ghost',
                     name=f'FGHC-{order_label}',
                     additional_settings={**order_settings, **FGHC_SETTINGS, **SIZE_SETTINGS},
                     should_keep_dump=True,
                     additional_csv_data={'Order': order_label},
                     progress_console=progress.console)

            progress.update(order_progress, advance=1)


#* ---------------------------------------------------------------------------
#* RankMap: the resolution sweep
#* ---------------------------------------------------------------------------
#*
#* RankMap projects every candidate allocation from the shadow rankings instead of running
#* ghost caches, so the allocation quantum can be made much finer than the 16 quanta the ghost
#* climbers are limited to. This sweep runs the same pipeline at quanta of 2, 4, 8, ... items.
#*
#* The shadow rankings depend only on the trace, the cache size and the block types - not on how
#* the cache is split between the blocks. One recording therefore serves every resolution, so the
#* sweep records it once on the cheapest run and replays it for all the others.
#*
#* Note the block types: RankMap ranks LRU, LFU and LBU. The LA-LRU / LA-LFU blocks the ghost
#* climber experiments use have no shadow ranking implemented yet, so this is a different pipeline
#* and its numbers are not directly comparable to the FGHC ones.

#* How many cache-capacities worth of requests pass between adaptation decisions. The sweep varies
#* this as a second axis: a short interval adapts quickly but scores each candidate on less evidence.
RANKMAP_DECISION_MULTIPLIERS = [2, 5, 10, 20]
RANKMAP_HALF_LIFE_MULTIPLIER = 50

#* Roughly a hundred bytes of allocation tree per candidate, so this keeps it to a few hundred MB.
RANKMAP_MAX_CANDIDATES = 2_000_000

RANKMAP_BLOCK_TYPES = ["LRU", "LFU", "LBU"]

RANKMAP_BURST_SETTINGS = {"pipeline.burst.aging-window-size" : 50,
                          "pipeline.burst.age-smoothing" : 0.0025,
                          "pipeline.burst.number-of-partitions" : 4,
                          "pipeline.burst.type" : "normal",
                          "pipeline.burst.sketch.eps" : 0.0001,
                          "pipeline.burst.sketch.confidence" : 0.99}


def rankmap_lfu_probation_size(cache_size: int) -> int:
    """
    The LFU shadow board's probation segment, pinned for the whole sweep. LfuBlock keeps one quantum
    on probation, so left to follow the quantum every resolution would have a different LFU board and
    need its own recording. One quantum of the default NUM_OF_QUANTA grid is the board the coarse
    RankMap design (and the FGHC experiments' LfuBlock) uses.
    """
    return max(1, cache_size // NUM_OF_QUANTA)


def rank_data_dir(cache_size: int) -> Path:
    """One directory per (trace, cache size), so 'has this been recorded yet' is a directory check."""
    return Path(RESULTS_DIR) / 'rank-data' / f'{OUTPUT_SUFFIX}'


def rankmap_snapshot_interval(cache_size: int, decision_multipliers: list) -> int:
    """
    Board snapshots are only read at a decision, so the recording only has to carry them often
    enough that every decision interval in the sweep lands on one. Every interval is a multiple of
    the capacity times the greatest common divisor of the multipliers, and taking the gcd keeps the
    recording as small as it can be while still serving all of them from one file.
    """
    from math import gcd
    from functools import reduce

    return reduce(gcd, decision_multipliers) * cache_size


def rankmap_recording(recordings: Path, cache_size: int, snapshot_interval: int):
    """
    A recording at this size that serves the whole sweep: its snapshot interval has to divide every
    decision interval, which is exactly dividing their gcd. RankMap picks the recording itself, by
    the same rule plus the fingerprint of the boards, so this only decides whether to make one.

    A recording is a pair made by one run: the .rankdata ranks and snapshots, which is all the
    hit-ratio objective needs, and the .benefit latency windows beside it. Without both it is not
    a recording.
    """
    if not recordings.is_dir():
        return None

    prefix = f'boards.C{cache_size}.{"-".join(RANKMAP_BLOCK_TYPES)}.S'
    for recording in sorted(recordings.glob(f'{prefix}*.rankdata')):
        interval = recording.name[len(prefix):].split('.', 1)[0]
        if (interval.isdigit() and snapshot_interval % int(interval) == 0
                and recording.with_suffix('.benefit').is_file()):
            return recording

    return None


def even_quotas(num_of_quanta: int, num_of_blocks: int) -> list:
    """An even split, handing any remainder to the middle blocks - 16 over 3 gives 5, 6, 5."""
    quotas = [num_of_quanta // num_of_blocks] * num_of_blocks
    for i in range(num_of_quanta % num_of_blocks):
        quotas[(num_of_blocks // 2 + i) % num_of_blocks] += 1

    return quotas


def rankmap_resolutions(cache_size: int, num_of_blocks: int, max_candidates: int) -> list:
    """
    The feasible quanta sizes, finest first: 2 items, 4 items, 8 items and so on.

    Every candidate allocation is scored on every request, and the number of them is the
    compositions of the quanta count into the blocks, so the finest resolutions stop being
    affordable well before the quantum reaches one item on a large cache.
    """
    from math import comb

    resolutions = []
    quantum = 2
    while quantum <= cache_size:
        if cache_size % quantum == 0:
            quanta = cache_size // quantum
            if quanta >= num_of_blocks:
                candidates = comb(quanta + num_of_blocks - 1, num_of_blocks - 1)
                if candidates <= max_candidates:
                    resolutions.append((quantum, quanta, candidates))
        quantum *= 2

    return resolutions


def rankmap_settings(cache_size: int, quantum_size: int, num_of_quanta: int, objective: str,
                     decision_multiplier: int, snapshot_interval: int,
                     precompute_mode: str, decision_log: str) -> dict:
    quotas = even_quotas(num_of_quanta, len(RANKMAP_BLOCK_TYPES))

    settings = {"pipeline.num-of-blocks" : len(RANKMAP_BLOCK_TYPES),
                "pipeline.num-of-quanta" : num_of_quanta,
                "pipeline.quantum-size" : quantum_size,
                **RANKMAP_BURST_SETTINGS,
                'rank-map.objective' : objective,
                'rank-map.decision-multiplier' : decision_multiplier,
                'rank-map.half-life-multiplier' : RANKMAP_HALF_LIFE_MULTIPLIER,
                'rank-map.validate-oracle' : False,
                'rank-map.lfu-board-probation-size' : rankmap_lfu_probation_size(cache_size),
                'rank-map.decision-log' : decision_log,
                'rank-map.precompute.mode' : precompute_mode,
                'rank-map.precompute.directory' : str(rank_data_dir(cache_size)),
                'rank-map.precompute.snapshot-interval' : snapshot_interval}

    for idx, block_type in enumerate(RANKMAP_BLOCK_TYPES):
        settings[f"pipeline.blocks.{idx}.type"] = block_type
        settings[f"pipeline.blocks.{idx}.quota"] = quotas[idx]
        for key, value in BLOCK_EXTRA_SETTINGS.get(block_type, {}).items():
            settings[f"pipeline.blocks.{idx}.{key}"] = value

    return settings


def run_rankmap_one(fname: str, trace_name: str, cache_size: int, quantum_size: int,
                    num_of_quanta: int, candidates: int, objective: str,
                    decision_multiplier: int, snapshot_interval: int, precompute_mode: str,
                    force: bool, progress_console = None, csv_filename: str = None) -> None:
    if csv_filename is None:
        csv_filename = f'RankMap-{objective}-q{quantum_size}-d{decision_multiplier}-{OUTPUT_SUFFIX}'

    settings = rankmap_settings(cache_size, quantum_size, num_of_quanta, objective,
                                decision_multiplier, snapshot_interval, precompute_mode,
                                decision_log=f'{RESULTS_DIR}/{csv_filename}.decisions.csv')

    run_test(fname, trace_name, cache_size, csv_filename, 'rank_map',
             name=f'{objective}-q{quantum_size}-d{decision_multiplier}',
             additional_settings=settings,
             additional_csv_data={'Quantum Size' : quantum_size,
                                  'Num Of Quanta' : num_of_quanta,
                                  'Candidates' : candidates,
                                  'Objective' : objective,
                                  'Decision Multiplier' : decision_multiplier,
                                  'Decision Interval' : decision_multiplier * cache_size,
                                  'Blocks' : '-'.join(RANKMAP_BLOCK_TYPES)},
             should_keep_dump=False,
             force=force,
             progress_console=progress_console)


def run_rankmap_resolution_sweep(fname: str, trace_name: str, cache_size: int,
                                 objectives: list, decision_multipliers: list,
                                 max_candidates: int) -> None:
    num_of_blocks = len(RANKMAP_BLOCK_TYPES)
    snapshot_interval = rankmap_snapshot_interval(cache_size, decision_multipliers)
    resolutions = rankmap_resolutions(cache_size, num_of_blocks, max_candidates)

    if not resolutions:
        console.print(f'[bold red]No feasible RankMap resolution for size {cache_size} '
                      f'within {max_candidates} candidates')
        return

    finest = resolutions[0]
    coarsest = resolutions[-1]
    console.log(f'[bold #a98467]RankMap resolutions for size {cache_size}: '
                f'{finest[0]} items ({finest[2]:,} candidates) up to '
                f'{coarsest[0]} items ({coarsest[2]:,} candidates)')

    recordings = rank_data_dir(cache_size)
    recording = rankmap_recording(recordings, cache_size, snapshot_interval)

    #* Recording is a run of its own, not one of the results, and it records for both objectives at
    #* once: the ranks for hit ratio and the latency windows for latency, in two files. Every result - the coarsest resolution
    #* included - then replays that one recording, so they all score against identical rankings and
    #* each run only reads the parts it needs: the snapshots at its own decisions, and as many depth
    #* boundaries as its quanta have. The coarsest resolution has the smallest allocation tree, so it
    #* is the cheapest configuration to drive the recording with.
    if recording is not None:
        console.log(f'[bold #adc178]Reusing the shadow rankings in {recording}')
    else:
        quantum_size, num_of_quanta, candidates = coarsest
        console.log(f'[bold #adc178]Recording the shadow rankings into {recordings} '
                    f'(snapshots every {snapshot_interval} requests)')
        #* The CSV lands beside the recording, as a record of the run that made it, and out of the
        #* results so nothing mistakes it for a sweep point.
        run_rankmap_one(fname, trace_name, cache_size, quantum_size, num_of_quanta, candidates,
                        'hit-ratio', snapshot_interval // cache_size, snapshot_interval,
                        precompute_mode='write', force=True,
                        csv_filename=str(Path('rank-data') / OUTPUT_SUFFIX / f'recording-S{snapshot_interval}'))

        recording = rankmap_recording(recordings, cache_size, snapshot_interval)
        if recording is None:
            console.print(f'[bold red]The recording run left no recording in {recordings}')
            exit(1)

    with Progress() as progress:
        total = len(resolutions) * len(objectives) * len(decision_multipliers)
        sweep = progress.add_task('[bold #bedcfe]RankMap resolutions', total=total, start=True)

        for objective in objectives:
            for decision_multiplier in decision_multipliers:
                #* Finest first, so the expensive end is reached while the recording is already warm.
                for quantum_size, num_of_quanta, candidates in resolutions:
                    run_rankmap_one(fname, trace_name, cache_size, quantum_size, num_of_quanta,
                                    candidates, objective, decision_multiplier, snapshot_interval,
                                    precompute_mode='read', force=False,
                                    progress_console=progress.console)
                    progress.update(sweep, advance=1)


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
    parser.add_argument('--run-adaptive-grid-search', help="Run the adaptive hill climber (FGHC) starting from every possible quota configuration", action='store_true', required=False)
    parser.add_argument('--run-exploring-grid-search', help="Run the exploring hill climber (ESHC) starting from every possible quota configuration", action='store_true', required=False)
    parser.add_argument('--run-statistics-grid-search', help="Run the statistics-based climber (SBC) starting from every possible quota configuration", action='store_true', required=False)
    parser.add_argument('--reorder-grid-search', help="Run grid search over all 6 permutations of LRU/LFU/LBU block order", action='store_true', required=False)
    parser.add_argument('--run-adaptive-pipeline-reordered', help="Run FGHC on all 6 permutations of LRU/LFU/LBU block order with equal starting quotas", action='store_true', required=False)
    parser.add_argument('--run-other', help="Run comparison algorithms, not including LHD and LRB", action='store_true', required=False)
    parser.add_argument('--run-rankmap-resolutions', help="Run RankMap over quanta of 2, 4, 8, ... items, recording the shadow rankings once and replaying them", action='store_true', required=False)
    parser.add_argument('--rankmap-objective', help="What RankMap optimizes in the resolution sweep", choices=['latency', 'hit-ratio', 'both'], default='latency', required=False)
    parser.add_argument('--rankmap-max-candidates', help="Skip resolutions with more candidate allocations than this", type=int, default=RANKMAP_MAX_CANDIDATES, required=False)
    parser.add_argument('--rankmap-decision-multipliers', help="Comma separated decision intervals to sweep, each as a multiple of the cache size", type=str, default=','.join(str(m) for m in RANKMAP_DECISION_MULTIPLIERS), required=False)

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

    if args.run_adaptive_grid_search:
        run_adaptive_grid_search(file.name, trace_name, cache_size)

    if args.run_exploring_grid_search:
        run_exploring_grid_search(file.name, trace_name, cache_size)

    if args.run_statistics_grid_search:
        run_statistics_grid_search(file.name, trace_name, cache_size)

    if args.reorder_grid_search:
        run_reorder_grid_search(file.name, trace_name, cache_size)

    if args.run_adaptive_pipeline_reordered:
        run_adaptive_pipeline_reordered(file.name, trace_name, cache_size)
                
    if args.run_rankmap_resolutions:
        objectives = ['latency', 'hit-ratio'] if args.rankmap_objective == 'both' else [args.rankmap_objective]
        decision_multipliers = sorted({int(m) for m in args.rankmap_decision_multipliers.split(',') if m.strip()})
        run_rankmap_resolution_sweep(file.name, trace_name, cache_size, objectives,
                                     decision_multipliers, args.rankmap_max_candidates)

    if args.run_other:
        run_other(file.name, trace_name, cache_size)
        
    console.log("[bold #a3b18a]#####################\tDone\t#####################\n\n")


if __name__ == "__main__":
    main()
