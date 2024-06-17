#!/usr/bin/env c:\Users\Janko\Miniconda3\envs\stonks\python.exe

import sys
import pandas as pd
import sqlite3
from functions import *
from datetime import datetime

before = datetime.now()
args = sys.argv[1:]

conn = sqlite3.connect('main.db')
tablename = 'test'

if 'deep' in args:
    timestamps = pd.read_sql_query(f'SELECT DISTINCT time FROM {tablename}', conn)
    dates = [date_from_timestamp(ts) for ts in timestamps['time']]
    trading_days = get_trading_days(from_date=min(dates))[1:]
    missingdates = missingdates(dates, trading_days)
else:
    last_timestamp = pd.read_sql_query(f'SELECT MAX(time) FROM {tablename}', conn).iloc[0,0]
    last_date = date_from_timestamp(last_timestamp)
    missingdates = get_trading_days(from_date=last_date)[1:]

if len(missingdates) == 0:
        print('No missing dates')
        sys.exit()
elif len(missingdates) == 1:
    print(f'There is 1 missing date at {missingdates[0]}')
else:
    print(f'There are {len(missingdates)} missing dates between {missingdates[0]} and {missingdates[-1]}')

if date_separation(missingdates) > 7:
    print('NOTE: Missing dates are far apart: ', missingdates)

input = input('Do you wish to proceed? (y/n)')
if input.lower() != 'y':
    print('Exiting.')
    sys.exit()

new_data = datelist_to_df(missingdates, json=False)
new_data.columns = get_table_colnames(tablename)

if new_data is None:
    print('No new data')
    sys.exit()

new_data.to_sql(tablename, conn, if_exists='append', index=False)

after = datetime.now()
print('Total time elapsed: ' + (after - before).total_seconds())
