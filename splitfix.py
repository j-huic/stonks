import os
import json
import sqlite3
from datafunc import *
from functions import *

test = True
db_uri = os.getenv('DB_URI_SHORT')
conn = sqlite3.connect(db_uri)

db_tickers = get_all_tickers()

with open('splitignore.json') as file:
    ignore = json.load(file)
    
start_date = input('Input start date:\n')
end_date = input('Input end date:\n')

splits = clean_splits(get_splits(from_date=start_date, to_date=end_date))
splits.sort_values('date', ascending=True, inplace=True)
splits.reset_index(inplace=True, drop=True)
print(f'tickers to be checked: {splits.ticker.unique()}')

for i, row in tqdm(splits.iterrows()):
    try:
        ticker = row['ticker']
        ratio = row['from'] / row['to']
        date = row['date']

        if (ticker in ignore) or (ticker not in db_tickers):
            tqdm.write(f'skipping {ticker}')
            continue

        if test:
            msg = f'TESTMSG: Adjusted split for {ticker} on {date} with ratio {ratio}'
        else:
            # adjust_presplit_price_db(conn, ticker, ratio, date)
            msg = f'Adjusted split for {ticker} on {date} with ratio {ratio}'

        add_log(msg)
    except Exception as e:
        add_log(e, verbose=True)



