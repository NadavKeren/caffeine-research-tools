from policies import Policy
from pathlib import Path
import json
import subprocess
from pyhocon import ConfigFactory
from pyhocon import ConfigTree
from pyhocon import HOCONConverter
from enum import Enum
import pandas as pd

from typing import Union, List


with open(Path(__file__).parent / 'conf.json') as conf_file:
    local_conf = json.load(conf_file)
caffeine_root = Path(local_conf['caffeine_root'])
resources_path = Path(local_conf['resources']) if local_conf['resources'] else caffeine_root / 'simulator' / 'src' / 'main' / 'resources' / 'com' / 'github' / 'benmanes' / 'caffeine' / 'cache' / 'simulator' / 'parser'
output_path = Path(local_conf['output']) if local_conf['output'] else Path.cwd()
output_csvs_path = output_path / 'csvs'


class Admission(Enum):
    ALWAYS = 'Always'
    TINY_LFU = 'TinyLfu'


def single_run(policy, trace_file:str, size:int, trace_folder:str,
               trace_format:str, additional_settings:dict={}, name:str | None=None,
               save:bool=True, reuse:bool=False, verbose:bool=False, readonly:bool=False,
               seed:int=1033096058):

    name = name if name else f'{trace_file}-{size}-{policy}'
    policy = Policy[policy]

    conf_path = caffeine_root / 'simulator' / 'src' / 'main' / 'resources'
    conf_file = conf_path / 'application.conf'

    if not output_csvs_path.exists():
        output_csvs_path.mkdir(parents=True, exist_ok=True)

    run_simulator_cmd = './gradlew simulator:run -x caffeine:compileJava -x caffeine:compileCodeGenJava -PjvmArgs=-Xmx8g'

    if conf_file.exists():
        conf = ConfigFactory.parse_file(str(conf_file))
    else:
        conf = ConfigFactory.parse_string("""
                                          caffeine {
                                            simulator {
                                            }
                                          }
                                          """)
        
    simulator : ConfigTree = conf['caffeine']['simulator']

    simulator.put('files.paths', [ str(resources_path / trace_folder / trace_file) ])

    simulator.put('files.format', trace_format)
    simulator.put('maximum-size', size)
    simulator.put('policies', [ policy.value ])
    simulator.put('admission', [ Admission.ALWAYS.value ])
    simulator.put('random-seed', seed)

    if verbose:
        simulator.put('report.format', 'table')
        simulator.put('report.output', 'console')
    else:
        simulator.put('report.format', 'csv')
        simulator.put('report.output', str(output_csvs_path / f'{name}.csv'))

    for k,v in additional_settings.items():
        simulator.put(k,v)

    with open(conf_file, 'w') as f:
        f.write(HOCONConverter.to_hocon(conf))
    if (not reuse or not Path(simulator['report']['output']).is_file()) and not readonly:
        retcode = subprocess.call(run_simulator_cmd, shell = True, cwd = str(caffeine_root), stdout = subprocess.DEVNULL if not verbose else None)
        if (not retcode == 0):
            return False

    if not verbose:
        with open(simulator['report']['output'], 'r') as csvfile:
            results = pd.read_csv(csvfile)

        if not save:
            Path(simulator['report']['output']).unlink()

        return results

if __name__ == '__main__':
    print("This is a library file, please run experiments from another script.")
