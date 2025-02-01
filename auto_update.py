import sys
import pandas as pd
import sqlite3
from functions import *
from datetime import datetime
import logging

before = datetime.now()

log_file = '/home/janko/stocklens/updater.log'
logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(message)s')
        
db_uri = ('/home/janko/stocklens/main.db')
tablename = 'stocks'
conn = sqlite3.connect(db_uri)

last_timestamp = pd.read_sql_query(f'SELECT MAX(timestamp) FROM {tablename}', conn).iloc[0,0]
last_date = date_from_timestamp(last_timestamp)
missingdates = get_trading_days(from_date=last_date)[1:]

if len(missingdates) == 0:
        print('No missing dates')
        after = datetime.now()
        logging.info('No new data')
        sys.exit()
elif len(missingdates) == 1:
    logging.info(f'1 missing date: {missingdates[0]}')
else:
    logging.info(f'{len(missingdates)} missing dates between {missingdates[0]} and {missingdates[-1]}')

# checking for scattered dates (indicative of missing data)
if date_separation(missingdates) > 4:
   logging.info('NOTE: Missing dates are far apart: %s', missingdates)

# missing data into pandas dataframe
new_data = datelist_to_df_parallel(missingdates, json=False, max_workers=5, noprint=True)

if new_data is None:
    logging.info('Data for %s not yet available', missingdates)
    sys.exit()

# new date column, align colnames, and append
new_data['date'] = new_data['t'].apply(date_from_timestamp)
new_data.columns = get_table_colnames(table=tablename, database=db_uri)
new_data.to_sql(tablename, conn, if_exists='append', index=False)
dl_dates = new_data['date'].unique()

after = datetime.now()

logging.info('Successfully downloaded %d files for trading day %s; Time elapsed: %s seconds', len(dl_dates), dl_dates, str(round((after - before).total_seconds(), 2)))
