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

def crossings(series, value=0):
    if value == 0:
        value = np.mean(series)

    diff = series - value
    signs = np.sign(diff)
    sign_changes = np.diff(signs)
    crossings = np.count_nonzero(sign_changes)

    return crossings
    
def get_ranks(df):
    if 'value' not in df.columns: df['value'] = df['close'] * df['volume']
    latest = df[df['date'] == df['date'].max()].copy()
    latest.sort_values('value', ascending=False, inplace=True)
    order = latest['ticker'].tolist()
    rankdict = dict(zip(order, range(len(order))))

    return df['ticker'].map(rankdict)
    
def get_constrained_volatile(df, l = 0.85, u = 1.15):
    df = df.copy()
    df['return'] = df['close'].groupby(df['ticker']).pct_change()
    df['cumret'] = df.groupby('ticker')['return'].transform(lambda x: (1 + x).cumprod())

    sds = df.groupby('ticker')['return'].std()
    crs = df.groupby('ticker')['cumret'].last()

    between = [t for t, v in crs.items() if l < v < u]
    between_sds = {key : value for key, value in sds.items() if key in between}
    bs_sorted = sorted(between_sds.items(), key=lambda item:item[1], reverse=True)

    return bs_sorted

def last_n_months(df, n):
    latest = pd.to_datetime(df['date'].max())
    cutoff = latest - pd.DateOffset(months=n)
    cutoff_str = cutoff.strftime('%Y-%m-%d')
    return df[df['date'] > cutoff_str]