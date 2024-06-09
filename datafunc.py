import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt

def plot(df, ticker, var='close'):
    ticker = ticker.upper()
    df = df.set_index('date', inplace=False)
    df[df.ticker == ticker][var].plot()
    plt.xticks(rotation=45)
    plt.show()

def getdistance(vector, n=20):
    ma = talib.MA(vector, timeperiod=n)
    stdev = talib.STDDEV(vector, timeperiod=n, nbdev=1)
    return (vector - ma) / stdev

def talib_wrap(group, func, colname, *args, **kwargs):
    values = group[colname].values
    result = func(values, *args, **kwargs)
    return result

def gethighestdistance(df, n=20):
    df['distance'] = df['close'].groupby(df['ticker']).transform(getdistance)
    last = df[df['date'] == df['date'].max()].copy()
    lowest = last.nsmallest(n, 'distance').loc[:,['ticker', 'distance']]
    highest = last.nlargest(n, 'distance').loc[:,['ticker', 'distance']]
    return lowest, highest

def printhighestdistance(df, n=20):
    lowest, highest = gethighestdistance(df, n)
    print(str(n) + ' Lowest stocks by distance:')
    print(lowest.to_string(index=False))
    print('\n' + str(n) + ' Highest stocks by distance:')
    print(highest.to_string(index=False))

def cumret(vector):
    returns = vector.pct_change()
    return (1 + returns).cumprod()

def ranking(ticker, rankdict):
    if ticker not in rankdict:
        return None
    return int(rankdict[ticker])

def hasduplicates(df, get=False, cols=['ticker', 'date']):
    duplicates = df.duplicated(cols)
    if not get:
        return duplicates.any()
    else: 
        return set(df[duplicates][cols[1]])