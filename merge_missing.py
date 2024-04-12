import pandas as pd
import os
from datetime import datetime
import csv
import sys

if len(sys.argv) == 1: filename = 'all_dailies.csv'
else: filename = sys.argv[1]

tempfile = 'temp.csv'

def merge_dailies(filenames):
    datelist = [datetime.strptime(f.split('/')[1].split('.')[0], '%Y-%m-%d').date() for f in filenames]
    dflist = [pd.read_csv(f) for f in filenames]

    for i,j in zip(dflist, datelist):
        i['date'] = j
    
    all = pd.concat(dflist)

    return all

def get_last_line(file_name):
    with open(file_name, 'rb') as f:
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        last_line = f.readline().decode()
    return last_line

def get_last_date(filename):
    most_recent_date_string = get_last_line(filename).split(",")[-1].strip('\r\n"')
    most_recent_date = datetime.date(datetime.strptime(most_recent_date_string, "%Y-%m-%d"))

    return most_recent_date

def append_csv(oldfile, newfile):
    with open(oldfile, 'a', newline='') as outfile:
        writer = csv.writer(outfile)
        with open(newfile, 'r') as infile:
            reader = csv.reader(infile)
            next(reader)
            for row in reader:
                writer.writerow(row)

def checkmissing(file='all_dailies.csv', dir='day_aggs'):
    dir='testaggs'
    files = os.listdir(dir)
    filedates = [datetime.strptime(f.split('.')[0], '%Y-%m-%d').date() for f in files]
    most_recent_date = get_last_date(file)
    recentfiles = [(i,j) for i,j in zip(files, filedates) if j > most_recent_date]
    
    if len(recentfiles) == 0: return None
    output = {'filenames': [t[0] for t in recentfiles], 'dates': [t[1] for t in recentfiles]}

    return output

missingfiles = checkmissing(filename)
if missingfiles is None: sys.exit("No missing files.")

missing = merge_dailies(['day_aggs/'+f for f in missingfiles['filenames']])
missing.to_csv(tempfile, mode='a', index=False)
append_csv(filename, tempfile)