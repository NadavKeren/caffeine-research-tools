import mmh3

from os.path import *
from os import path, listdir, makedirs

from typing import Generator, List, Dict

from utils import Timer

IGNORED_URL_TAGS = ["?search=", "&search=", "User+talk", "User_talk","User:", "Talk:", "&diff=", 
                    "&action=rollback", "Special:Watchlist"]

IGNORED_PREFIXES = ["wiki/Special:Search", "w/query.php","wiki/Talk:", "wiki/Special:AutoLogin",
                    "Special:UserLogin", "w/api.php", "error:"]

OUTPUT_DIR = './processed'
OUTPUT_FILE_PREFIX = f'{OUTPUT_DIR}/processed_'


def parseLine(entry: str):   
    url = entry.split(' ')[2]
    
    for prefix in IGNORED_PREFIXES:
        if url.startswith(prefix):
            return None
        for tag in IGNORED_URL_TAGS:
            if tag in url:
                return None
    
    return url


def _processFile(fname: str):
    with open(f'./input/{fname}', encoding='utf-8',errors='replace') as wiki:
        with open(f'{OUTPUT_FILE_PREFIX}{fname}','w') as outputFile:
            line = wiki.readline()
            count = 1
            while line:
                url = parseLine(line)
                if url != None:
                    key = str(int.from_bytes(mmh3.hash_bytes(url)[-8:],'big'))
                    outputFile.write('%s\n'%key)
                line = wiki.readline()
                count += 1


def processFiles(files : List[str]):
    print(f'processing the files: {files}\n')
    for file in files:
        if not path.exists(f'{OUTPUT_FILE_PREFIX}{file}'):
            print(f'start processing {file}')
            with Timer():
                _processFile(file)
                
            print(f'done processing: {file}')
    print('done processing files\n\n')


def main():
    input_files_paths = [f for f in listdir('./input') if isfile(join('./input', f)) and f.startswith('wiki')] # can be downloaded from http://www.wikibench.eu/wiki and other wiki traces too
    
    makedirs(OUTPUT_DIR, exist_ok=True)
    
    processFiles(input_files_paths)

     
if __name__ == '__main__':
    main()
