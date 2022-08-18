import argparse
import dns
import dns.resolver
from dns.exception import Timeout as TimeoutException
import pandas as pd
import numpy as np
from numpy.random import default_rng

# ! May become deprecated, can use a wrapper code over requests.get
from urllib.request import urlretrieve
from os import rename
from os.path import exists
from pathlib import Path
from zipfile import ZipFile
from tqdm import tqdm
from typing import Tuple, List, Literal

from datetime import timedelta, datetime
from time import sleep


BASE_DIR = f'{Path(__file__).parent.resolve()}'
ALEXA_RANKING_PATH = f'{BASE_DIR}/alexa_top_1m.csv'
UMBRELLA_RANKING_PATH = f'{BASE_DIR}/umbrella_top_1m.csv'
ALEXA_DOWNLOAD_URL = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
UMBRELLA_DOWNLOAD_URL = 'http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip'

# * Setting the Timeout and Lifetime of each query to 2 seconds, removing some of the outliers.
dns_resolver = dns.resolver.Resolver()
dns_resolver.lifetime = 3
dns_resolver.timeout = 3
dns_resolver.nameservers = []


def set_dns_nameserver(option : Literal['google_main', 'google_secondary', 'opendns_main', 'opendns_secondary']):
    if (option == 'google_main'):
        dns_resolver.nameservers = ['8.8.8.8']
    elif (option == 'google_secondary'):
        dns_resolver.nameservers = ['8.8.4.4']
    elif (option == 'opendns_main'):
        dns_resolver.nameservers = ['208.67.222.222']
    elif (option == 'opendns_secondary'):
        dns_resolver.nameservers = ['208.67.220.220']
    else:
        raise Exception(f'No option {option} for DNS name server')


def download_alexa_top_entries_if_needed():
    download_and_extract(ALEXA_DOWNLOAD_URL, ALEXA_RANKING_PATH)
        
        
def download_umbrella_top_entries_if_needed():
    download_and_extract(UMBRELLA_DOWNLOAD_URL, UMBRELLA_RANKING_PATH)
        
        
def download_and_extract(url : str, dest_path : str):
    if (not exists(dest_path)):
        zip_path = f'{BASE_DIR}/top-1m.csv.zip'
        
        urlretrieve(url, filename=zip_path)
        
        with ZipFile(zip_path, 'r') as zip:
            zip.extractall(BASE_DIR)
            rename(f'{BASE_DIR}/top-1m.csv', dest_path)
                    

def time_name_resolution(domain_entry : pd.Series):
    domain = domain_entry['domain']
    
    send_time = datetime.now()
    try:
        if 'resolve' in dir(dns.resolver):
            result = dns_resolver.resolve(domain, 'A', search=True)
        else:
            result = dns_resolver.query(domain, 'A')
        
    except TimeoutException as e:
        return dns_resolver.timeout * 1000
    except Exception as e:
        return np.nan
    
    receive_time = datetime.now()
    
    stat_duration = receive_time - send_time
    return stat_duration.total_seconds() * 1000


def parse_arguments():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d', '--download', help="Download the alexa top 1m list if needed", action='store_true')
    parser.add_argument('-o', '--origin', help='The origin of the domain rankings', choices=['alexa', 'umbrella'], required=True)
    # parser.add_argument('--high', help='The highest rank to stat', type=int, required=True)
    # parser.add_argument('--low', help='The lowest rank to stat', type=int, required=True)
    parser.add_argument('-v', '--verbose', help='Prints progress bar for computation', action='store_true')
    parser.add_argument('--dns-server', help='Choose the DNS name server to query', choices=['google_main', 'google_secondary', 'opendns_main', 'opendns_secondary'], required=True)
    parser.add_argument('-b', '--batch-size', help='The batch size to sample on each iteration', default=200, type=int)
    
    return parser.parse_args()


def prepare_ranking_dfs(origin : Literal['alexa', 'umbrella'], ranks : List[Tuple[int, int]]) -> List[pd.DataFrame]:
    ranking_path = ALEXA_RANKING_PATH if origin == 'alexa' else UMBRELLA_RANKING_PATH
    
    ranking_df : pd.DataFrame = pd.read_csv(ranking_path, names=['rank', 'domain'])
    
    return [ranking_df.loc[(ranking_df['rank'] >= high) & (ranking_df['rank'] <= low)] for high, low in ranks]


def run_timing_iteration(curr_sampled_df : pd.DataFrame, curr_ranks : Tuple[int, int], iteration : int, 
                         origin : str, dns_server : str, verbose : bool = False):
    if (verbose):
            tqdm.pandas()
            curr_sampled_df['latency'] = curr_sampled_df.progress_apply(time_name_resolution, axis=1)
    else:
        curr_sampled_df['latency'] = curr_sampled_df.apply(time_name_resolution, axis=1)
    
    curr_sampled_df = curr_sampled_df.assign(iteration=f'{iteration}')  
    
    output_csv_path = f'{BASE_DIR}/time_rank_{origin}_{dns_server}_{curr_ranks[0]}_{curr_ranks[1]}.csv'

    should_append_header = not exists(output_csv_path)
    curr_sampled_df.to_csv(output_csv_path, mode='a', header=should_append_header, index=False)


def main():
    args = parse_arguments()
    
    if (args.download):
        if (args.origin == 'alexa'):
            download_alexa_top_entries_if_needed()
        else:
            download_umbrella_top_entries_if_needed()
            
    set_dns_nameserver(args.dns_server)
    
    ranks = list(zip([1, 10000, 100000, 400000], [2000, 12000, 102000, 402000]))
    ranking_dfs_lst = prepare_ranking_dfs(args.origin, ranks)
    
    rand_gen = default_rng()
    iteration = 0
    
    while (True):
        curr_ranking_df = ranking_dfs_lst[iteration % len(ranking_dfs_lst)]
        curr_ranks = ranks[iteration % len(ranking_dfs_lst)]
        indeces = rand_gen.choice(len(curr_ranking_df), size=args.batch_size, replace=False)
        
        curr_sampled_df = curr_ranking_df.iloc[indeces].copy(deep=True)
        
        run_timing_iteration(curr_sampled_df, curr_ranks, iteration, args.origin, args.dns_server)
        
        sleep(60)
        iteration += 1
            
    
if __name__ == '__main__':
    main()