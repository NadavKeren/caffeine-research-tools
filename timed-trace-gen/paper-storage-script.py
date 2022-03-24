import os

input_files = ['085.gcc.din']
ratios = ['1#2']
drives = ['SSD1#HDD2']
formats = ['address'] 

commands = [f'python3 address-trace-maker.py {infile} {ratio} {drvs} format={f}' for infile,ratio,drvs,f in zip(input_files,ratios,drives,formats)]

for cmd in commands:
    os.system(cmd)