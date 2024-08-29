import sqlite3
import pandas as pd
from functions import *

datelist = get_trading_days()
max_workers = input('Enter max workers (): ')
df = datelist_to_df_parallel(datelist, max_workers=max_workers)
df['date'] = df['t'].apply(date_from_timestamp)

database = input('Enter database name: ')
table = input('Enter table name: ')

conn = sqlite3.connect(database)
df.columns = get_table_colnames(table)
df.to_sql(table, conn, if_exists='append', index=False)
conn.close()

