import sqlite3
import pandas as pd
from functions import *

database = input('Enter database name: ')
if database == 'default':
    database = '/home/janko/stocklens/main.db'
table = 'stocks'

conn = sqlite3.connect(database)

maxdays = int(input('Enter maximum number of days to download: '))
datelist = get_trading_days()[:maxdays]
max_workers = int(input('Enter max workers (): '))
df = datelist_to_df_parallel(datelist, max_workers=max_workers)
df['date'] = df['t'].apply(date_from_timestamp)

df.columns = get_table_colnames(table=table, database=database)
df.to_sql(table, conn, if_exists='append', index=False)
conn.close()

