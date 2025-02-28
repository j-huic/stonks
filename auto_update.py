import sys
import pandas as pd
import sqlite3
import logging
import os
from datetime import datetime
from datafunc import *
from functions import *

def main():
    before = datetime.now()

    # log config
    log_file = '/home/janko/stocklens/updater.log'
    logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S',
            format='%[(asctime)s] (%(levelname)s): %(message)s')
            
    try: 
        # db conn
        db_uri = os.getenv('DB_URI_SHORT')
        tablename = 'stocks'
        conn = sqlite3.connect(db_uri)

        # checking missing data
        last_date = get_max_value('date', conn)
        missingdates = get_trading_days(from_date=last_date)[1:]
        activetickers = get_all_tickers(conn=conn, onlyactive=True)

        if len(missingdates) == 0:
                logging.info('No new data')
                sys.exit()
        elif len(missingdates) == 1:
            logging.info(f'1 missing date: {missingdates[0]}')
        else:
            logging.info(
                f'{len(missingdates)} missing dates between ' 
                f'{missingdates[0]} and {missingdates[-1]}'
            )

        # missing data into pandas dataframe
        new_data = datelist_to_df_parallel(missingdates, json=False, max_workers=5, noprint=True)

        if new_data is None:
            logging.info(f'Data for {missingdates} not yet available')
            sys.exit()

        # new date column, align colnames, and append
        new_data['date'] = new_data['t'].apply(date_from_timestamp)
        new_data.columns = get_table_colnames(table=tablename, database=db_uri)
        new_data.to_sql(tablename, conn, if_exists='append', index=False)
        dl_dates = new_data['date'].unique()
        after_dl = datetime.now()

        logging.info(
            f'Successfully downloaded data for {len(dl_dates)} trading day(s) ({dl_dates}); '
            f'Time elapsed: {format_timedelta(after_dl - before)} seconds'
        )

        # checking for splits since last update and adjusting
        splits = clean_splits(get_splits(from_date=dl_dates.min(), to_date=dl_dates.max()))
        inboth = in_both(splits.ticker.unique(), activetickers)
        logging.info(
            f'Total of {splits.shape[0]} splits between {splits.date.min()} and '
            f'{splits.date.max()} with {len(inboth)} of those for stocks in the database'
        )

        for _, row in splits.iterrows():
            ticker = row['ticker']
            ratio = row['from'] / row['to']
            date = row['date']

            if ticker not in activetickers:
                continue

            adjust_presplit_price_db(conn, ticker, ratio, date)
            logging.info(f'Adjusted split for {ticker} on {date} with ratio {ratio}')

        after = datetime.now()
        logging.info(
            f'Performed split fix operations in {format_timedelta(after - after_dl)}s; '
            f'Total time elapsed: {format_timedelta(after - before)}s'

        )

    except Exception as e:
        logging.error(f'Error has occurred: {str(e)}', exc_info=True)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
