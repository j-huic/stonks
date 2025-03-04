import sys
import pandas as pd
import sqlite3
from functions import *
from datafunc import *
from datetime import datetime
import os

before = datetime.now()
args = sys.argv[1:]

db_uri = os.getenv('DB_URI_SHORT') 
tablename = 'stocks'
conn = sqlite3.connect(db_uri)

# if deep as argument, check all previous dates for missing data, otherwise since latest
if 'deep' in args:
    timestamps = pd.read_sql_query(f'SELECT DISTINCT timestamp FROM {tablename}', conn)
    dates = [date_from_timestamp(ts) for ts in timestamps['timestamp']]
    trading_days = get_trading_days(from_date=min(dates))[1:]
    missingdates = missingdates(dates, trading_days)
else:
    last_date = get_max_value('date', conn)
    missingdates = get_trading_days(from_date=last_date)[1:]

if len(missingdates) == 0:
        print('No missing dates')
        sys.exit()
elif len(missingdates) == 1:
    print(f'1 missing date at {missingdates[0]}')
else:
    print(f'{len(missingdates)} missing dates between {missingdates[0]} and {missingdates[-1]}')

proceed = input('Proceed? (y/n)\n')
if proceed.lower() != 'y':
    print('Exiting.')
    sys.exit()

new_data = datelist_to_df_parallel(missingdates, json=False, max_workers=5)

if new_data is None:
    print(f'Data for {missingdates} not available')
    sys.exit()

# new date column, align colnames, and append
new_data['date'] = new_data['t'].apply(date_from_timestamp)
new_data.columns = get_table_colnames(table=tablename, database=db_uri)
dl_dates = new_data['date'].unique()
print('Updating Database...')

# # check for splits
splits = clean_splits(get_splits(from_date=dl_dates.min(), to_date=dl_dates.max()))
activetickers = get_all_tickers(conn=conn, onlyactive=True)
inboth = in_both(splits.ticker.unique(), activetickers)
print(f'{len(inboth)} splits since last update')

for _, row in tqdm(splits.iterrows()):
    ratio = row['from'] / row['to']
    ticker = row['ticker']
    date = row['date']

    if ticker not in activetickers:
        adjust_presplit_price_db(conn, ticker, ratio, date)

#

new_data.to_sql(tablename, conn, if_exists='append', index=False)

after = datetime.now()
print('Total time elapsed: ' + str(round((after - before).total_seconds(), 2)) + ' seconds')
