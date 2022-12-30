import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import glob
from os import makedirs, path

from typing import List

import argparse
import tqdm

OUTPUT_FORMAT='png'

QUANTILES_TO_CALCULATE = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.85, 0.875, 0.9, 0.925, 0.95, 0.96, 0.97, 0.98, 0.99]


def load_df(datafile : str) -> pd.DataFrame:
    df = pd.read_csv(datafile)
    
    df.dropna(inplace=True, subset=['latency'])
    
    df = df.sort_values(['rank', 'domain'])
    
    return df


def calculate_quantiles(df : pd.DataFrame) -> List:
    return [df['latency'].quantile(q=quan) for quan in QUANTILES_TO_CALCULATE]


def plot_latency_scattering(df : pd.DataFrame, output_dir : str = None) -> None:
    variances = df.groupby('rank').var()
    
    plt.figure()
    plt.scatter(df['rank'], df['latency'], s=10, c=df['time'], cmap='Purples', edgecolors='#f5ebe0')
    plt.xlabel('domain rank')
    plt.ylabel('latency (ms)')
    plt.grid(True)
    
    if (not output_dir is None):
        plt.savefig(f'{output_dir}/scatterplot.{OUTPUT_FORMAT}', dpi=300)
    else:
        plt.show()
    
    plt.close()

def plot_latency_quantiles(quantiles : List, output_dir : str = None) -> None:
    plt.figure()
    plt.plot(QUANTILES_TO_CALCULATE, quantiles)
    plt.xlabel('percentile (%)')
    plt.ylabel('latency (ms)')
    plt.grid(True)
    
    if (not output_dir is None):
        plt.savefig(f'{output_dir}/quantiles.{OUTPUT_FORMAT}', dpi=300)
    else:
        plt.show()
        
    plt.close()
    

def create_quantiles_table(quantiles : List, output_dir : str = None) -> None:
    plt.figure()
    fig, ax = plt.subplots()
    fig.patch.set_visible(False)
    ax.axis('off')
    ax.axis('tight')

    df = pd.DataFrame({'quantile': QUANTILES_TO_CALCULATE, 'latency': quantiles})
    df.update(df['latency'].apply('{:,.2f}'.format))

    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center', colLoc='center', colColours=['#219ebc', '#219ebc'])

    fig.tight_layout()
    if (not output_dir is None):
        plt.savefig(f'{output_dir}/quantiles_table.{OUTPUT_FORMAT}', dpi=300)
    else:
        plt.show()

    plt.close()
    

def process_file(fname : str, input_dir : str, output_dir : str):
    df : pd.DataFrame = load_df(f'{input_dir}/{fname}')
    quantiles = calculate_quantiles(df)
    
    curr_output_dir = f'{output_dir}/{fname.rsplit(".csv", 1)[0]}'
    makedirs(curr_output_dir, exist_ok=True)
    
    plot_latency_scattering(df, curr_output_dir)
    plot_latency_quantiles(quantiles, curr_output_dir)
    create_quantiles_table(quantiles, curr_output_dir)
    

def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-i', '--input-dir', help="The path of the directory containing the csv data files", type=str, required=True)
    parser.add_argument('-o', '--output-dir', help='The path to the desired output directory', type=str, required=True)
    
    args = parser.parse_args()
    
    input_file_names = [path.basename(f) for f in glob.glob(f'{args.input_dir}/*.csv')]
    makedirs(args.output_dir, exist_ok=True)
    
    for fname, _ in zip(input_file_names, tqdm.trange(len(input_file_names))):
        process_file(fname, args.input_dir, args.output_dir)
        

if __name__ == '__main__':
    main()