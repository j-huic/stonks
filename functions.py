import pandas as pd
import os
import csv
import requests
import sqlite3
import pandas_market_calendars as mcal
import connectorx as cx
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


def date_from_filename(filename):
    date_part = filename.split('/')[1].split('.')[0]
    date = datetime.strptime(date_part, '%Y-%m-%d').date()

    return date


def merge_dailies(filenames, path='day_aggs/'):
    filenames = [path + f for f in filenames]
    datelist = [date_from_filename(f) for f in filenames]
    dflist = [pd.read_csv(f) for f in filenames]

    for df, date in zip(dflist, datelist):
        df['date'] = date
    all = pd.concat(dflist)

    return all


def get_last_line(filename='all_dailies.csv'):
    with open(filename, 'rb') as f:
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        last_line = f.readline().decode()

    return last_line


def get_last_date(filename='all_dailies.csv'):
    most_recent_date_string = get_last_line(filename).split(",")[-1].strip('\r\n"')
    parsed_date = datetime.strptime(most_recent_date_string, "%Y-%m-%d")
    most_recent_date = datetime.date(parsed_date)

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
    dates = [datetime.strptime(f.split('.')[0], '%Y-%m-%d').date() for f in files]
    most_recent_date = get_last_date(file)
    recent_files = [
        (file, date) for file, date in zip(files, dates)
        if date > most_recent_date
    ]

    if len(recent_files) == 0:
        return None

    output = {
        'filenames': [t[0] for t in recent_files],
        'dates': [t[1] for t in recent_files]
        }

    return output


def date_from_filename_(filename, datetime=False):
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


def in_both(a, b):
    return [item for item in a if item in b]


def daily_agg(date, apikey=None, output='data'):
    if apikey is None:
        apikey = os.getenv('POLYGON_APIKEY_1')
    url = (
        f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/'
        f'{date}?apiKey={apikey}'
        )
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
        return pd.DataFrame(response['results'])
    elif output == 'all':
        return response
    else:
        return None


def single_stock(ticker, from_date, to_date, apikey=None, df=True):
    if apikey is None:
        apikey = os.getenv('POLYGON_APIKEY_1')

    url = (
        f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/'
        f'{from_date}/{to_date}?apiKey={apikey}'
    )
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
        df = pd.read_csv(filename, usecols=[0])  # noqa: F841
    else:
        df = pd.read_csv(filename)  # noqa: F841

    after = datetime.now()
    print((after - before).total_seconds())


def datelist_to_df(datelist, json=False):
    before = datetime.now()

    alljson = []
    for date in tqdm(datelist):
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
            print(
                f'{n_files} file request done '
                f'in {minutes} minutes and {round(seconds, 2)} seconds')
    else:
        if minutes == 0:
            print(f'{n_files} file requests done in {seconds} seconds')
        else:
            print(
                f'{n_files} file requests done '
                f'in {minutes} minutes and {round(seconds, 2)} seconds'
            )


def date_from_timestamp(timestamp, intraday=False):
    dt = datetime.fromtimestamp(timestamp / 1000)
    if not intraday:
        return dt.strftime('%Y-%m-%d')
    else:
        return dt.strftime('%Y-%m-%d %H:%M')


def timestamp_to_date(timestamp, intraday=False):
    return date_from_timestamp(timestamp, intraday)


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
    columns = [
        f"CAST({col} AS FLOAT) AS {col}"
        if col == 'volume'
        else col for col in vars
    ]
    select = ', '.join(columns)
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


def get_top_stocks_cx(n=5000, from_date='2023-01-01', vars=['ticker', 'date', 'close']):
    query = get_top_stocks_query(n, from_date, vars)
    data = pd.DataFrame(get_data(query))
    data.columns = vars

    return data


def get_max_value(var, conn, table='stocks'):
    c = conn.cursor()
    max = c.execute(f'SELECT MAX({var}) FROM {table};').fetchone()[0]

    return max


def option_handler(apikey=None, noqm=False, **kwargs):
    if apikey is None:
        apikey = os.getenv('POLYGON_APIKEY_1')
    kwargs['apiKey'] = apikey

    if 'from_date' in kwargs:
        kwargs['execution_date.gte'] = kwargs['from_date']
        del kwargs['from_date']
    if 'to_date' in kwargs:
        kwargs['execution_date.lte'] = kwargs['to_date']
        del kwargs['to_date']
    if 'from_date_ex' in kwargs:
        kwargs['execution_date.gt'] = kwargs['from_date_ex']
        del kwargs['from_date_ex']
    if 'to_date_ex' in kwargs:
        kwargs['execution_date.lt'] = kwargs['to_date_ex']
        del kwargs['to_date_ex']

    options = [f'{key}={value}' for key, value in kwargs.items()]
    output = '?' + '&'.join(options) if not noqm else '&' + '&'.join(options)

    return output


def make_request(base_url, output='json', apikey=None, **kwargs):
    if apikey is None:
        apikey = os.getenv('POLYGON_APIKEY_1')

    if base_url[:5] != 'https':
        base_url = 'https://api.polygon.io/' + base_url

    option_string = (
        option_handler(apikey, **kwargs)
        if 'cursor' not in base_url
        else option_handler(apikey, noqm=True, **kwargs)
    )
    url = base_url + option_string

    if output == 'json':
        return requests.get(url).json()
    else:
        return requests.get(url)


def make_request_full(url, limit=1000, **kwargs):
    output = []
    response = make_request(url, limit=min(limit, 1000), **kwargs)
    output.extend(response['results'])

    if limit <= 1000:
        return pd.DataFrame(output)
    else:
        remaining = limit - 1000

        while 'next_url' in response.keys() and remaining > 0:
            response = make_request(response['next_url'], limit=min(remaining, 1000), **kwargs)
            output.extend(response['results'])
            remaining -= 1000

    return pd.DataFrame(output)

def get_splits(output='json', **kwargs):
    output = []
    response = make_request('v3/reference/splits', **kwargs, limit=1000)
    output.extend(response['results'])

    while 'next_url' in response.keys():
        response = make_request(response['next_url'])
        output.extend(response['results'])

    return pd.DataFrame(output)


def get_stocks(tickers, from_date, cols=['date', 'ticker', 'close'], db_uri=None):
    if db_uri is None:
        conn = sqlite3.connect('main.db')
    else:
        conn = sqlite3.connect(db_uri)

    columns = ', '.join(cols) 
    sql_tickers = ', '.join([f"'{t}'" for t in tickers])

    query = f'''
    SELECT {columns}
    FROM Stocks
    WHERE ticker in ({sql_tickers})
    AND date >= '{from_date}'
    '''

    df = pd.read_sql_query(query, conn)

    return df


def get_crypto(ticker='BTCUSD', from_date='2024-01-01', all_cols=False, apikey=None):
    if apikey is None:
        apikey = os.getenv('POLYGON_APIKEY_1')

    to_date = datetime.now().strftime('%Y-%m-%d')
    url = (
        f'https://api.polygon.io/v2/aggs/ticker/X:{ticker}/range/1/day/'
        f'{from_date}/{to_date}?apiKey={apikey}'
    )

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

    return df if all_cols else df[['date', 'ticker', 'close']]


def get_ticker_type(ticker, apikey=None):
    if apikey is None:
        apikey = os.getenv('POLYGON_APIKEY_1')

    url = f'https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={apikey}'
    response = requests.get(url).json()

    return response


def get_stock_only_tickers(tickers=None, apikey=None, params=None):
    if apikey is None:
        apikey = os.getenv('POLYGON_APIKEY_1')

    if params is None:
        params = {
            'market': 'stocks',
            'type': 'CS',  # CS = Common Stock
            'active': 'true',
            'limit': 1000
        }

    url = f'https://api.polygon.io/v3/reference/tickers?apiKey={apikey}'
    output = []
    response = requests.get(url, params=params).json()
    output.extend(response['results'])

    while ('next_url' in response.keys()):
        response = make_request(response['next_url'])
        output.extend(response['results'])

    return output


def adjust_presplit_price(df, ticker, ratio, cols=['close'], invcols=None):
    for col in cols:
        df.loc[df['ticker'] == ticker, col] *= ratio
    
    if invcols is not None:
        for col in invcols:
            df.loc[df['ticker'] == ticker, col] /= ratio

    return df


def adjust_presplit_price_db(conn, ticker, ratio, date=None):
    cursor = conn.cursor()
    query = f'''
        UPDATE stocks
        SET
            volume_weighted = volume_weighted * ?,
            open = open * ?,
            close = close * ?,
            high = high * ?,
            low = low * ?,
            volume = volume / ?
        WHERE ticker = ?
    '''
    params = (ratio,) * 6 + (ticker,)

    if date is not None:
        query += ' AND date < ?'
        params = params + (date,)

    cursor.execute(query, tuple(params))
    conn.commit()


def add_log(message, filename='info.log'):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(filename, 'a') as file:
        file.write(f'[{timestamp}]: {message}\n')


def get_all_tickers(db_uri=None, conn=None, table='stocks', onlyactive=False):
    if conn is None:
        db_uri = os.getenv('DB_URI_SHORT')
        conn = sqlite3.connect(db_uri)

    cursor = conn.cursor()

    query = f'''
    SELECT DISTINCT ticker
    FROM {table}
    '''

    if onlyactive:
        query += f''' 
        WHERE date = (
            SELECT max(date)
            FROM stocks
        )
        '''
    
    response = cursor.execute(query).fetchall()
    
    return [item[0] for item in response]


def get_intraday(ticker, timespan, multiplier, from_date, to_date):
    if to_date is None:
       to_date = datetime.now().strftime('%Y-%m-%d')

    url = (
        f'v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/'
        f'{from_date}/{to_date}'
        )
    
    output = []
    response = make_request(url)
    output.extend(response['results'])

    while('next_url' in response.keys()):
        response = make_request(response['next_url'])
        output.extend(response['results'])

    return pd.DataFrame(output)


def clean_results(df, cols=None, intraday=False):
    if cols is None:
        cols = ['date', 'close']
        
    df['date'] = df['t'].apply(lambda x: date_from_timestamp(x, intraday))
    df.columns = ['volume', 'vw', 'open', 'close', 'high', 
                  'low', 'timestamp', 'transactions', 'date']
                  
    return df[cols].copy()

