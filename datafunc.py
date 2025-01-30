import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import talib

from functions import get_top_stocks_cx, get_top_stocks_query

def plot(df, ticker='', ma=5, var='close', forceticker=0, plot=True):
    if forceticker == 0:
        ticker = ticker.upper()
    else: 
        ticker = forceticker

    df = df.set_index('date', inplace=False)
    df[df.ticker == ticker][var].rolling(ma).mean().plot()
    plt.xticks(rotation=45)
    
    if plot:
        plt.show()
    else:
        return plt

def plot_list(df, tickers, ma=5, var='close'):
    n = len(tickers)
    ncol = 3
    nrow = n // ncol + (n % ncol > 0)
    
    fig, axs = plt.subplots(nrow, ncol, figsize=(15, nrow * 5))
    axs = axs.flatten()
    
    for i, ticker in enumerate(tickers):
        ax = axs[i]
        ticker_df = df[df.ticker == ticker].set_index('date', inplace=False)
        ticker_df[var].rolling(ma).mean().plot(ax=ax)
        ax.set_title(ticker)
        ax.tick_params(axis='x', rotation=45)
    
    for j in range(i + 1, len(axs)):
        axs[j].set_visible(False)
    
    plt.tight_layout()
    plt.show()

def plot_ax(df, ticker, ax, ma=5, var='close'):
    df = df.set_index('date', inplace=False)
    df[df.ticker == ticker][var].rolling(ma).mean().plot(ax=ax)
    plt.xticks(rotation=45)

def plot_series(series, ma=5):
    sma = talib.SMA(np.array(series), timeperiod=ma)
    plt.plot(series)

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

def get_crossings(series, value=0, ma=1):
    if value == 0:
        value = np.mean(series)

    diff = series.rolling(ma).mean() - value
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

def get_tickers_between(df, l=0.75, u=1.25, var='cumret'):
    crs = df.groupby('ticker')[var].last()
    between = [t for t in crs.index if crs[t] >= l and crs[t] <= u]
    return between

def get_between(df, l=0.75, u=1.25, var='cumret'):
    tickers = get_tickers_between(df, l=l, u=u, var=var)
    return df[df['ticker'].isin(tickers)].copy()

def normalize(vector):
    return (vector - vector.mean()) / vector.std()

def add_n_week_low(df, targetcol, n):
    nwklow = str(n) + '-wk-low'
    df[nwklow] = df.groupby('ticker')[targetcol].transform(lambda x: talib.MIN(x, timeperiod=n*5))

    return df

def add_n_week_high(df, targetcol, n):
    nwkhigh = str(n) + '-wk-high'
    df[nwkhigh] = df.groupby('ticker')[targetcol].transform(lambda x: talib.MAX(x, timeperiod=n*5))

    return df

def n_week_low(df, n, targetcol='close', groupcol='ticker'):
    nwl = df.groupby(groupcol)[targetcol].transform(lambda x: talib.MIN(x, timeperiod=n*5))

    return nwl
    
def n_week_high(df, n, targetcol='close', groupcol='ticker'):
    nwh = df.groupby(groupcol)[targetcol].transform(lambda x: talib.MAX(x, timeperiod=n*5))

    return nwh

def hist(df, var, bin=30, clip=0.95, latest=False):
    if latest:
        df = df[df['date'] == df['date'].max()]

    lower = df[var].quantile((1-clip)/2)
    upper = df[var].quantile(clip+(1-clip)/2)
    clipped = df[var].clip(lower=lower, upper=upper)
    clipped.hist()

def remove_outliers(df, vars, quantile=0.99, onesided=False):
    for var in vars:
        if onesided:
            upper = df[var].quantile(quantile)
            df = df[df[var] < upper]
        else:
            lower = df[var].quantile((1 - quantile) / 2)
            upper = df[var].quantile(quantile + (1 - quantile) / 2)
            df = df[(df[var] > lower) & (df[var] < upper)]
        print([lower, upper])

    return df

def remove_max(df, var):
    return df[df[var] != df[var].max()]

def remove_min(df, var):
    return df[df[var] != df[var].min()]