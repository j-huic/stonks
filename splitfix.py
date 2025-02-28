import pickle
import json
from datafunc import *
from functions import *
import os
import sqlite3

test = False
db_uri = os.getenv('DB_URI_SHORT')
conn = sqlite3.connect(db_uri)

db_tickers = get_all_tickers()

with open('recent_splits.pkl', 'rb') as file:
    allsplits = pickle.load(file)
with open('splitignore.json') as file:
    ignore = json.load(file)
    
# splits = allsplits[allsplits.date.between('2024-08-30', '2024-09-10')].copy()
splits = allsplits[allsplits.date == '2024-08-30'].copy()
splits.sort_values('date', ascending=True, inplace=True)
splits.reset_index(inplace=True, drop=True)

for i, row in splits.iterrows():
    ticker = row['ticker']
    ratio = row['from'] / row['to']
    date = row['date']

    if (ticker in ignore) or (ticker not in db_tickers):
        print(f'skipping {ticker}')
        continue

    print(row)
    print('\n')

    if i > 10:
        break

    if test:
        msg = f'TESTMSG: Adjusted split for {ticker} on {date} with ratio {ratio}'
    else:
        adjust_presplit_price_db(conn, ticker, ratio, date)
        msg = f'Adjusted split for {ticker} on {date} with ratio {ratio}'

    add_log(msg, verbose=True)



