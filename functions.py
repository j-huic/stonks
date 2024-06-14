import pandas as pd
import os
from datetime import datetime, timedelta
import csv
import sys
import requests
import pandas_market_calendars as mcal

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
    status = response['status']

    if status == 'NOT_AUTHORIZED':
        print(response['message'])
        return None
    elif status == 'OK':
        if response['queryCount'] == 0:
            print('No data available for ' + date)
            return None
    
    if output == 'data': 
        return response['results']
    elif output == 'df': 
        return pd.DataFrame(data['results'])
    elif output == 'all':
        return response
    else:
        return None
    
def single_stock(ticker, from_date, to_date, apikey, df=True):
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{from_date}/{to_date}?apiKey={apikey}'
    response = requests.get(url)
    data = response.json()

    if df:
        df = pd.DataFrame(data['results'])
    return data

def date_from_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

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

def datelist_to_df(datelist):
    before = datetime.now()
    
    alljson = []
    for i, date in enumerate(tqdm(datelist)):
        data = daily_agg(date)
        if data is not None:
            alljson.extend(data)
    
    after = datetime.now()
    totalseconds = (after - before).total_seconds()
    minutes = totalseconds // 60
    seconds = totalseconds % 60
    print(f'{len(datelist)} file requests done in {minutes} minutes and {round(seconds, 2)} seconds')
    
    return pd.DataFrame(alljson)

def datestring_from_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')