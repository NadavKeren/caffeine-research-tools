# access-times-trace-generator

This repository holds two easy to use scripts, that are used to generate traces of requests from different formats into a consistent format which includes access times for each request in the original trace.

## Types of input traces

### Wikipedia traces

These traces are taken from [wikibench](http://www.wikibench.eu/?page_id=60).

### Memory addresses and Numeric key based traces

#### Supported formats
* **gradle** - key is hex number on first field in line with fields seprated by spaces
* **address** - address is hex number on second field in line with fields seprated by spaces
* **arc** - address is number on first field and size in bytes is second field in line with fields seprated by spaces, the two fields are used to create blocks starting at diffrent addresses, this is the format used by trace in the [ARC paper](https://www.usenix.org/legacy/events/fast03/tech/full_papers/megiddo/megiddo.pdf)
* **oltp** - key is number on first field in line with fields seprated by commas
* **lirs** - key is number and is the only data in the line

#### **Where to download traces**
This is a partial list, more can be found online at public traces repository:
* **gradle** - available at [caffeine repo](https://github.com/ben-manes/caffeine)
* **address**- available at [caffeine repo](https://github.com/ben-manes/caffeine)
* **oltp** - available at [UMassTraceRepository](http://traces.cs.umass.edu/index.php/Storage/Storage)
* **lirs** - available at [caffeine repo](https://github.com/ben-manes/caffeine)

## Usage of Wikipedia script
Before running the script you need to place the input traces from [wikibench](http://www.wikibench.eu/?page_id=60) in the input directory.

Then you need to update the `wiki-trace-maker.py` file for your wanted paramters.

`TODO: add command line arguments to control parameters`

The required changes are in the following lines
```python
biases = [[0.8,0.15,0.05]] 
ranges = [(10,31),(120,181),(350,451)]] 
for wikifile in ['wiki1190207720']:
    ...
```
1. In the wiki files list enter the strings of the file names you downloaded and placed in the `input` directory.
2. In the `biases` list put the ratio of each range of times from the ranges found in the `ranges` list at the same index.

For example in the code above:
* 80% of the requests will be fetched in times in the range 10ms - 30ms
* 15% of the requests will be fetched in times in the range 120ms - 180ms
* 5% of the requests will be fetched in times in the range 350ms - 450ms

#### Then you can run the following:
<pre>
python3 wiki-trace-maker.py
</pre>

## Usage of other script
Run:
<pre>
python3 paper-storage-script.py
</pre>
To generate `GCC` example trace with access times based on SSD and HDD.

You can change the input files, formats and times distributions easily in the file `paper-storage-script.py`.

In order to add new types of Drives to be used you may edit the dictionary found at the top of the `address-trace-maker.py` file.
