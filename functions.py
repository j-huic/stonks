import pandas as pd
import os
import csv
import sys
import requests
import sqlite3
import pandas_market_calendars as mcal
import connectorx as cx
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def merge_dailies(filenames, path='day_aggs/'):
    filenames = [path + f for f in filenames]
    datelist = [datetime.strptime(f.split('/')[1].split('.')[0], '%Y-%m-%d').date() for f in filenames]
    dflist = [pd.read_csv(f) for f in filenames]

    for i,j in zip(dflist, datelist):
        i['date'] = j
    
    all = pd.concat(dflist)

    return all

def get_last_line(file_name='all_dailies.csv'):
    with open(file_name, 'rb') as f:
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        last_line = f.readline().decode()
    return last_line

def get_last_date(filename='all_dailies.csv'):
    most_recent_date_string = get_last_line(filename).split(",")[-1].strip('\r\n"')
    most_recent_date = datetime.date(datetime.strptime(most_recent_date_string, "%Y-%m-%d"))

    return most_recent_date

def append_csv(oldfile, newfile):
    with open(oldfile, 'a', newline='') as outfile:
        writer = csv.writer(outfile)
        with open(newfile, 'r') as infile:
            reader = csv.reader(infile)
            next(reader)
            for row in reader:
                writer.writerow(row)

def checkmissing(file='all_dailies.csv', dir='day_aggs'):
    files = os.listdir(dir)
    filedates = [datetime.strptime(f.split('.')[0], '%Y-%m-%d').date() for f in files]
    most_recent_date = get_last_date(file)
    recentfiles = [(i,j) for i,j in zip(files, filedates) if j > most_recent_date]
    
    if len(recentfiles) == 0: return None
    output = {'filenames': [t[0] for t in recentfiles], 'dates': [t[1] for t in recentfiles]}

    return output

def datefromfilename(filename, datetime=False):
    items = filename.split('/')
    lastitem = items[-1]
    datestring = lastitem.split('.')[0]
    
    if datetime:
        return datetime.strptime(datestring, '%Y-%m-%d')
    else:
        return datestring

def hasduplicates(df, get=False, cols=['ticker', 'date']):
    duplicates = df.duplicated(cols)
    if not get:
        return duplicates.any()
    else: 
        return set(df[duplicates][cols[1]])

def missingdates(existingdates, availabledates):
    return [x for x in availabledates if x not in existingdates]

def daily_agg(date, apikey='3CenRhJBzNqh2_C_5S38pOyt3ozLvQDm', output='data'):
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?apiKey={apikey}'
    response = requests.get(url)
    response = response.json()

    if output == 'response':
        return response

    status = response['status']

    if status == 'NOT_AUTHORIZED':
        print(response['message'])
        return None
    elif status == 'OK' or status == 'DELAYED':
        if response['queryCount'] == 0:
            return None
    else:
        return None
    
    if output == 'data': 
        return response['results']
    elif output == 'df': 
        return pd.DataFrame(data['results'])
    elif output == 'all':
        return response
    else:
        return None
    
def single_stock(ticker, from_date, to_date, apikey='3CenRhJBzNqh2_C_5S38pOyt3ozLvQDm', df=True):
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}?apiKey={apikey}'
    response = requests.get(url)
    data = response.json()

    if df:
        df = pd.DataFrame(data['results'])
        return df
    else:
        return data

def get_trading_days(from_date=None, to_date=None):
    nyse = mcal.get_calendar('NYSE')

    if to_date is None:
        to_date = datetime.now().date()
    if from_date is None:
        from_date = datetime.now().date() - timedelta(days=365*5)

    trading_days = nyse.valid_days(start_date=from_date, end_date=to_date)
    trading_dates = [day.strftime('%Y-%m-%d') for day in trading_days]
    
    return trading_dates

def loadtest(filename, onecol=False):
    before = datetime.now()
    
    if onecol:
        df = pd.read_csv(filename, usecols=[0])
    else:
        df = pd.read_csv(filename)

    after = datetime.now()
    print((after - before).total_seconds())

def datelist_to_df(datelist, json=False):
    before = datetime.now()
    
    alljson = []
    for i, date in enumerate(tqdm(datelist)):
        data = daily_agg(date)
        if data is not None:
            alljson.extend(data)
    
    after = datetime.now()
    file_request_print(len(datelist), after - before)
    
    if len(alljson) == 0:
        return None
    elif json:
        return alljson
    else:
        return pd.DataFrame(alljson)

def datelist_to_df_parallel(datelist, max_workers=11, json=False, noprint=False):
    before = datetime.now()
    
    alljson = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(daily_agg, date) for date in datelist]
        for future in tqdm(as_completed(futures), total=len(datelist), disable=noprint):
            try:
                data = future.result()
                if data is not None:
                    alljson.extend(data)
            except Exception as e:
                print(f"Error fetching data: {e}")
    
    after = datetime.now()

    if not noprint:
    	file_request_print(len(datelist), after - before)
    
    if len(alljson) == 0:
        return None
    elif json:
        return alljson
    else:
        return pd.DataFrame(alljson).sort_values('t')

def file_request_print(n_files, timedelta):
    seconds = timedelta.total_seconds()
    minutes = seconds // 60
    seconds = round(seconds % 60, 4)

    if n_files == 1:
        if minutes == 0:
            print(f'{n_files} file request done in {seconds} seconds')
        else:
            print(f'{n_files} file request done in {minutes} minutes and {round(seconds, 2)} seconds')
    else:
        if minutes == 0:
            print(f'{n_files} file requests done in {seconds} seconds')
        else:
            print(f'{n_files} file requests done in {minutes} minutes and {round(seconds, 2)} seconds')


def date_from_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

def timestamp_to_date(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

def timestamp_from_date(datestring):
    return int(datetime.strptime(datestring, '%Y-%m-%d').timestamp() * 1000)

def date_to_timestamp(datestring):
    return int(datetime.strptime(datestring, '%Y-%m-%d').timestamp() * 1000)

def get_table_colnames(table='stocks', database='main.db'):
    conn = sqlite3.connect(database)
    c = conn.cursor()
    info = c.execute(f'PRAGMA table_info({table})').fetchall()
    
    return [i[1] for i in info]

def timedelta_to_str(td):
    seconds = td.total_seconds()
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    if hours != 0:
        return f'{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds'
    elif minutes > 0:
        return f'{int(minutes)} minutes, {int(seconds)} seconds'
    else:
        return f'{round(seconds, 4)} seconds'

def date_separation(datelist):
    if len(datelist) < 2:
        return 0
        
    datestrings = sorted(list(set(datelist)))
    dates = [datetime.strptime(date, '%Y-%m-%d') for date in datestrings]
    timedeltas = [dates[i] - dates[i-1] for i in range(1, len(dates))]

    return max(timedeltas).days

def get_data(query):
    conn = sqlite3.connect('main.db')
    c = conn.cursor()

    c.execute(query)
    data = c.fetchall()
    return data

def get_data_cx(query):
    df = cx.read_sql('sqlite://main.db', query)

    return df

def get_top_stocks_query(n, since, vars):
    select = ', '.join(vars)
    timestamp = date_to_timestamp(since)
    command = f'''
        SELECT {select} FROM stocks
        WHERE timestamp > {timestamp} and ticker in (
            SELECT ticker FROM stocks
            WHERE timestamp = (
                SELECT MAX(timestamp) FROM stocks
            )
            ORDER BY volume * volume_weighted DESC
            LIMIT {n}
        )
        '''
    return command

def get_top_stocks_cx(n=5000, since='2023-01-01', vars=['ticker', 'date', 'close']):
    query = get_top_stocks_query(n, since, vars)
    data = pd.DataFrame(get_data(query))
    data.columns = vars

    return data

def get_top_stocks_cx(n=5000, since='2023-01-01', vars=['ticker', 'date', 'close']):
    query = get_top_stocks_query(n, since, vars)
    data = pd.DataFrame(get_data_cx(query))
    data.columns = vars

    return data

def get_max_value(var, database='main.db', table='stocks'):
    conn = sqlite3.connect(database)
    c = conn.cursor()
    max = c.execute(f'SELECT MAX({var}) FROM {table};').fetchone()[0]
    return max
    
def option_handler(apikey='3CenRhJBzNqh2_C_5S38pOyt3ozLvQDm', noqm=False, **kwargs):
    kwargs['apiKey'] = apikey

    if 'from_date' in kwargs:
        kwargs['execution_date.gte'] = kwargs['from_date']
        del kwargs['from_date']
    if 'to_date' in kwargs:
        kwargs['execution_date.lte'] = kwargs['to_date']
        del kwargs['to_date']
        
    options = [f'{key}={value}' for key, value in kwargs.items()]
    output = '?' + '&'.join(options) if not noqm else '&' + '&'.join(options)
    
    return output
        
def make_request(baseurl, output='json', apikey='3CenRhJBzNqh2_C_5S38pOyt3ozLvQDm', **kwargs):
    if baseurl[:5] != 'https': baseurl = 'https://api.polygon.io/' + baseurl
    
    optionstring = option_handler(apikey, **kwargs) if 'cursor' not in baseurl else option_handler(apikey, noqm=True, **kwargs)
    url = baseurl + optionstring
    
    if output == 'json':
        return requests.get(url).json()
    else:
        return requests.get(url)

def get_splits(output='json', **kwargs):
    output = []
    response = make_request('v3/reference/splits', **kwargs, limit=1000)
    output.extend(response['results'])

    while ('next_url' in response.keys()):
        response = make_request(response['next_url'])
        output.extend(response['results'])
        
    return pd.DataFrame(output)

def get_stocks(tickers, startDate, allCols=False):
    conn = sqlite3.connect('main.db')

    columns = '*' if allCols else 'date, ticker, close'
    sql_tickers = ', '.join([f"'{t}'" for t in tickers])

    query = f'''
    SELECT {columns} FROM stocks WHERE ticker in ({sql_tickers}) AND date >= '{startDate}'
    '''

    df = pd.read_sql_query(query, conn)
    return df

def get_crypto(ticker='BTCUSD', startDate = '2024-01-01', allCols=False, apiKey='3CenRhJBzNqh2_C_5S38pOyt3ozLvQDm'):
    endDate = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v2/aggs/ticker/X:{ticker}/range/1/day/{startDate}/{endDate}?apiKey=' + apiKey
    response = requests.get(url)
    df = pd.DataFrame(response.json()['results'])
    colmap = {
        'v' : 'volume',
        'wv' : 'volume_weighted',
        'o' : 'open',
        'c' : 'close',
        'h' : 'high',
        'l' : 'low',
        't' : 'timestamp',
        'n' : 'transactions'
    }
    df.rename(columns=colmap, inplace=True)
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime('%Y-%m-%d')
    df['ticker'] = ticker

    return df if allCols else df[['date', 'ticker', 'close']]