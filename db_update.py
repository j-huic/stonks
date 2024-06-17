#!/usr/bin/env c:\Users\Janko\Miniconda3\envs\stonks\python.exe

import sys
import pandas as pd
import sqlite3
from functions import *
from datetime import datetime

before = datetime.now()
args = sys.argv[1:]

conn = sqlite3.connect('main.db')
c = conn.cursor()

if 'deep' in args:
    timestamps = pd.read_sql_query('SELECT DISTINCT time FROM stocks', conn)
    dates = [date_from_timestamp(ts) for ts in timestamps['time']]
    trading_days = get_trading_days(from_date=min(dates))[1:]
    missingdates = missingdates(dates, trading_days)
    print(missingdates)
    if len(missingdates) == 0:
        print('No missing dates')
        sys.exit()
    if date_separation(missingdates) > 1:
        print('Missing dates are far apart: ', missingdates)
        input = input('Do you wish to proceed? (y/n) ')
        if input != 'y':
            sys.exit()
        
else:
    last_timestamp = pd.read_sql_query('SELECT MAX(time) FROM stocks', conn).iloc[0,0]
    last_date = date_from_timestamp(last_timestamp)
    missingdates = get_trading_days(from_date=last_date)[1:]

new_data = datelist_to_df(missingdates)

after = datetime.now()
print((after - before).total_seconds())

if new_data is None:
    print('No new data')