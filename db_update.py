#!/opt/homebrew/Caskroom/miniconda/base/envs/stonks/bin/python

import sys
import pandas as pd
import sqlite3
from functions import *
from datetime import datetime

before = datetime.now()
args = sys.argv[1:]

conn = sqlite3.connect('main.db')
tablename = 'stocks'

# if deep as argument, check all previous dates for missing data, otherwise since latest
if 'deep' in args:
    timestamps = pd.read_sql_query(f'SELECT DISTINCT timestamp FROM {tablename}', conn)
    dates = [date_from_timestamp(ts) for ts in timestamps['timestamp']]
    trading_days = get_trading_days(from_date=min(dates))[1:]
    missingdates = missingdates(dates, trading_days)
else:
    last_timestamp = pd.read_sql_query(f'SELECT MAX(timestamp) FROM {tablename}', conn).iloc[0,0]
    last_date = date_from_timestamp(last_timestamp)
    missingdates = get_trading_days(from_date=last_date)[1:]

if len(missingdates) == 0:
        print('No missing dates')
        after = datetime.now()
        print('Total time elapsed: ' + str(round((after - before).total_seconds(), 2)) + ' seconds')
        sys.exit()
elif len(missingdates) == 1:
    print(f'There is 1 missing date at {missingdates[0]}')
else:
    print(f'There are {len(missingdates)} missing dates between {missingdates[0]} and {missingdates[-1]}')

# checking for scattered dates (indicative of missing data)
if date_separation(missingdates) > 7:
    print('NOTE: Missing dates are far apart: ', missingdates)

input = input('Do you wish to proceed? (y/n)')
if input.lower() != 'y':
    print('Exiting.')
    sys.exit()

# missing data into pandas dataframe
new_data = datelist_to_df_parallel(missingdates, json=False)

if new_data is None:
    print('No new data')
    sys.exit()

# new date column, align colnames, and append
# new_data['date'] = new_data['timestamp'].apply(date_from_timestamp)
new_data['date'] = new_data['t'].apply(date_from_timestamp)
new_data.columns = get_table_colnames(tablename)

print('Updating Database...')
new_data.to_sql(tablename, conn, if_exists='append', index=False)

after = datetime.now()
print('Total time elapsed: ' + str(round((after - before).total_seconds(), 2)) + ' seconds')
