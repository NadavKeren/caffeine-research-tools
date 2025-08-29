import argparse
from pathlib import Path
import polars as pl
import matplotlib.pyplot as plt


def load_and_combine_csvs(directory : Path):        
    csv_files = list(directory.glob("*.csv"))
    
    dataframes = []
    
    for csv_file in csv_files:
        df = pl.read_csv(csv_file)
        dataframes.append(df)

    combined_df = pl.concat(dataframes, how="vertical")
    
    return combined_df


def create_latency_plot(df : pl.DataFrame, output_path: Path):
    df_sorted = df.select(["Cache Size", "Average Penalty"]).sort("Cache Size")
    
    cache_sizes = df_sorted["Cache Size"].to_list()
    avg_latencies = df_sorted["Average Penalty"].to_list()
    
    plt.figure(figsize=(10, 6))
    plt.plot(cache_sizes, avg_latencies, marker='o', linewidth=2, markersize=6)
    
    plt.xlabel("Cache Size", fontsize=12)
    plt.ylabel("Average Latency", fontsize=12)
    plt.grid(True, alpha=0.3)
    
    tick_values = [1 << i for i in range(7, 18)]
    plt.xscale('log', base=2)
    plt.xticks(tick_values)
    plt.tight_layout()
    
    plt.savefig(output_path.resolve())


def create_pareto(directory : Path):
    if not directory.exists():
        raise FileNotFoundError(f"Directory {directory.absolute} does not exist")
    
    if not directory.is_dir():
        raise NotADirectoryError(f"{directory.absolute} is not a directory")
    
    combined_df = load_and_combine_csvs(directory)
    
    create_latency_plot(combined_df, directory / "pareto.pdf")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--directory", help="Directory path containing CSV files")
    
    args = parser.parse_args()
    directory = Path(args.directory)
    
    create_pareto(directory)
    
if __name__ == "__main__":
    main()