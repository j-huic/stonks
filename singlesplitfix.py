import os
import sqlite3
from functions import *

db_uri = os.getenv('DB_URI_SHORT')
conn = sqlite3.connect(db_uri)

ticker = input("Enter Ticker:\n")
ratio = float(input("Enter ratio:\n"))
date = input("Enter date:\n")

adjust_presplit_price_db(conn, ticker, ratio, date) 

msg = f'Adjusted split for {ticker} on {date} with ratio {ratio}'
add_log(msg, verbose=True)

