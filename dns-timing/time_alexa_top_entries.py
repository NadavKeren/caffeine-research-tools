import argparse
import dns
import dns.resolver
import pandas as pd
from numpy import nan

# ! May become deprecated, can use a wrapper code over requests.get
from urllib.request import urlretrieve
from os import rename
from os.path import exists
from pathlib import Path
from zipfile import ZipFile

from datetime import timedelta, datetime

BASE_DIR = f'{Path(__file__).parent.resolve()}'
RANKING_PATH = f'{BASE_DIR}/alexa_top_1m.csv'
DOWNLOAD_PATH = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'

dns_resolver = dns.resolver.Resolver()
dns_resolver.nameservers = ['8.8.8.8']

def download_alexa_top_entries_if_needed():
    if (not exists(RANKING_PATH)):
        zip_path = f'{BASE_DIR}/top_1m.zip'
        
        urlretrieve(DOWNLOAD_PATH, filename=zip_path)
        
        with ZipFile(zip_path, 'r') as zip:
            zip.extractall(BASE_DIR)            
            rename(f'{BASE_DIR}/top-1m.csv', RANKING_PATH)
            

def time_name_resolution(domain_entry : pd.Series):
    domain = domain_entry['domain']
    
    send_time = datetime.now()
    try:
        result = dns_resolver.query(domain, 'A')
    except Exception as e:
        return nan
    
    receive_time = datetime.now()
    
    stat_duration = receive_time - send_time
    return stat_duration.total_seconds() * 1000


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d', '--download', help="Download the alexa top 1m list if needed", action='store_true')
    parser.add_argument('--high', help='The highest rank to stat', type=int, required=True)
    parser.add_argument('--low', help='The lowest rank to stat', type=int, required=True)
    
    args = parser.parse_args()
    
    if (args.download):
        download_alexa_top_entries_if_needed()
    
    ranking_df : pd.DataFrame = pd.read_csv(RANKING_PATH, names=['rank', 'domain'])
    
    ranking_df = ranking_df.loc[(ranking_df['rank'] >= args.high) & (ranking_df['rank'] <= args.low)]
    
    ranking_df['latency'] = ranking_df.apply(time_name_resolution, axis=1)
    
    output_csv_path = f'{BASE_DIR}/time_rank_{args.high}_{args.low}.csv'
    
    should_append_header = not exists(output_csv_path)
    ranking_df.to_csv(output_csv_path, mode='a', header=should_append_header, index=False)

if __name__ == '__main__':
    main()