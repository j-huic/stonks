import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import talib
from datetime import datetime


def plot(df, ticker, ma=[5], var='close', forceticker=False, plot=True, vert=None):
    if not forceticker:
        ticker = ticker.upper()
    else:
        ticker = forceticker

    df = df[df['ticker'] == ticker].copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date', inplace=False)

    for period in ma:
        df[df.ticker == ticker][var].rolling(period).mean().plot()

    plt.title(f'{ticker} - MA: {str(ma)}')
    plt.xticks(rotation=45)
    plt.xlabel('')

    if vert:
        plt.axvline(vert, color='red')

    if plot:
        plt.show()
    else:
        return plt


def plot_list(df, tickers, ma=5, var='close', fromzero=True):
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
        if fromzero:
            ax.set_ylim(bottom=0)

    for j in range(i + 1, len(axs)):
        axs[j].set_visible(False)

    plt.tight_layout()
    plt.show()


def plot_by_var(df, var, method, ascending=False, n=12, from_date=None):
    if method == 'now':
        now = df[df['date'] == df['date'].max()]

        if not ascending:
            tickerlist = list(now.nlargest(n, var)['ticker'])
        else:
            tickerlist = list(now.nsmallest(n, var)['ticker'])

        plot_list(df, tickerlist, ma=1)
    elif method == 'sum':
        if from_date is None: from_date = df['date'].min()
        sums = df[df['date'] > from_date].groupby('ticker')[var].sum().reset_index()
        srt = sums.sort_values(var, ascending=ascending).reset_index(drop=True)
        plot_list(df, srt.ticker.iloc[:n])


def plot_ax(df, ticker, ax, ma=5, var='close'):
    df = df.set_index('date', inplace=False)
    df[df.ticker == ticker][var].rolling(ma).mean().plot(ax=ax)
    plt.xticks(rotation=45)


def plot_series(series, ma=5):
    sma = talib.SMA(np.array(series), timeperiod=ma)
    plt.plot(series)
    plt.plot(sma)


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
    lowest = last.nsmallest(n, 'distance').loc[:, ['ticker', 'distance']]
    highest = last.nlargest(n, 'distance').loc[:, ['ticker', 'distance']]
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
    if 'value' not in df.columns:
        df['value'] = df['close'] * df['volume']
    latest = df[df['date'] == df['date'].max()].copy()
    latest.sort_values('value', ascending=False, inplace=True)
    order = latest['ticker'].tolist()
    rankdict = dict(zip(order, range(len(order))))

    return df['ticker'].map(rankdict)


def get_constrained_volatile(df, lower=0.85, upper=1.15):
    df = df.copy()
    df['return'] = df['close'].groupby(df['ticker']).pct_change()
    df['cumret'] = df.groupby('ticker')['return'].transform(lambda x: (1 + x).cumprod())

    sds = df.groupby('ticker')['return'].std()
    crs = df.groupby('ticker')['cumret'].last()

    between = [t for t, v in crs.items() if lower < v < upper]
    between_sds = {key : value for key, value in sds.items() if key in between}
    bs_sorted = sorted(between_sds.items(), key=lambda item: item[1], reverse=True)

    return bs_sorted


def last_n_months(df, n):
    latest = pd.to_datetime(df['date'].max())
    cutoff = latest - pd.DateOffset(months=n)
    cutoff_str = cutoff.strftime('%Y-%m-%d')
    return df[df['date'] > cutoff_str]


def get_tickers_between(df, lower=0.75, upper=1.25, var='cumret'):
    crs = df.groupby('ticker')[var].last()
    between = [t for t in crs.index if crs[t] >= lower and crs[t] <= upper]
    return between


def get_between(df, lower=0.75, upper=1.25, var='cumret'):
    tickers = get_tickers_between(df, lower=lower, upper=upper, var=var)
    return df[df['ticker'].isin(tickers)].copy()


def normalize(vector):
    return (vector - vector.mean()) / vector.std()


def add_n_week_low(df, target, n):
    nwklow = str(n) + '-wk-low'
    dfgroup = df.groupby('ticker')[target]
    df[nwklow] = dfgroup.transform(lambda x: talib.MIN(x, timeperiod=n*5))

    return df


def add_n_week_high(df, target, n):
    nwkhigh = str(n) + '-wk-high'
    dfgroup = df.groupby('ticker')[target]
    df[nwkhigh] = dfgroup.transform(lambda x: talib.MAX(x, timeperiod=n*5))

    return df


def n_week_low(df, n, target='close', group='ticker'):
    nwl = df.groupby(group)[target].transform(lambda x: talib.MIN(x, timeperiod=n*5))

    return nwl


def n_week_high(df, n, target='close', group='ticker'):
    nwh = df.groupby(group)[target].transform(lambda x: talib.MAX(x, timeperiod=n*5))

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


def add_sma(df, n, group='ticker', var='close'):
    sma = df.groupby(group)[var].transform(lambda x: talib.SMA(x, n))
    return sma


def add_cmax(df, var='close', group='ticker'):
    cmax = df.groupby(group)[var].transform(lambda x: x.cummax())
    return cmax


def add_cmin(df, var='close', group='ticker'):
    cmin = df.groupby(group)[var].transform(lambda x: x.cummin())
    return cmin


def add_ma_diff(df, n, group='ticker', var='close'):
    df = df.copy()
    df[f'ma{n}'] = add_sma(df, n, var=var, group=group)
    df[f'ma{n}_low'] = add_cmin(df, var=f'ma{n}', group=group)
    df[f'ma{n}_high'] = add_cmax(df, var=f'ma{n}', group=group)
    diff = df[f'ma{n}_low'] / df[f'ma{n}_high']

    return diff


def get_quantiles(vector, index=False):
    n = len(vector)
    quantiles = [sum(i > vector) / n for i in vector]

    if index:
        return (np.array(quantiles) * 100)
    else:
        return quantiles


def vectors_mean(vectors, weights, method='linear'):
    vectors = np.array(vectors)
    weights = np.array(weights)

    if method == 'linear':
        return np.average(vectors, axis=0, weights=weights)
    elif method == 'geometric':
        weighted_quantiles = np.power(vectors, weights[:, np.newaxis])
        geometric_mean_score = (
            np.prod(weighted_quantiles, axis=0) **
            (1 / np.sum(weights))
        )

        return geometric_mean_score


def add_score(df, vars, weights=None, method='linear'):
    # var_quantiles = [get_quantiles(df[var], index=True) for var in vars]
    var_quantiles = [df[var].rank(pct=True) for var in vars]

    if weights is None:
        score = np.mean(var_quantiles, axis=0)
    else:
        if len(vars) != len(weights):
            raise ValueError("Number of weights should match number of variables")

        score = vectors_mean(var_quantiles, weights, method)

    return score


def clean_splits(allsplits, from_date=None, to_date=None, cols=None, trim=False):
    if cols is None:
        cols = ['execution_date', 'split_from', 'split_to', 'ticker']
    output = allsplits[cols].copy()
    output.columns = ['date', 'from', 'to', 'ticker']

    if trim:
        if from_date is None:
            from_date = '2015-01-01'
        if to_date is None:
            to_date = datetime.now().strftime('%Y-%m-%d')

        output = output[output['date'].between(from_date, to_date)]

    return output


def get_split_ratio(splits, ticker):
    split = splits[splits.ticker == ticker].iloc[-1, :]
    ratio = split['from'] / split['to']

    return ratio


def get_split_date(splits, ticker):

    return splits[splits.ticker == ticker].date.iloc[-1]


def find_split(df, ticker, ratio):
    df = df[df.ticker == ticker].copy()
    df['return'] = df['close'].pct_change() + 1
    df['diff'] = abs(df['return'] - ratio)
    row = df.loc[df['diff'].idxmin()]

    return row['date']


def validate_split(ratio, ret):
    diff = abs(ratio / ret - 1)
    grade = None
    if (ratio > 3) & (ret > 3):
        if diff < 2:
            grade = 1
        else:
            grade = 0.75
    elif ratio > 1.5:
        if diff < 0.3:
            grade = 1
        elif diff < 0.5:
            grade = 0.5
        elif diff < 1.5:
            grade = 0.25
        else:
            grade = 0
    elif (ratio < 0.5) & (ret < 0.5):
        if diff < 0.3:
            grade = 1
        elif diff > 0.5:
            grade = 0.25
        elif diff > 1:
            grade = 0
    elif (ratio < 0.6) & (ret > 0.9):
        grade = 0
    elif ratio < 0.8:
        if diff < 0.1:
            grade = 1
        elif diff < 0.3:
            grade = 0.75
        else:
            grade = 0.25
    else:
        if diff < 0.1:
            grade = 1
        else:
            grade = 0.5

    return (grade, diff)


def is_in_both(list_one, list_two):
    output = []
    if len(list_one) > len(list_two):
        placeholder = list_one
        list_one = list_two
        list_two = placeholder

    for item in list_one:
        if item in list_two:
            output.append(item)

    return output


def get_ma_order(df, mas, strict=True, bearish=True):
    df = df.copy()
    mas.sort()
    for ma in mas:
        df[f'ma{ma}'] = add_sma(df, ma)

    for i, ma in enumerate(mas[:-1]):
        if bearish:
            df[f'madom{i}'] = df[f'ma{ma}'] < df[f'ma{mas[i + 1]}']
        else:
            df[f'madom{i}'] = df[f'ma{ma}'] > df[f'ma{mas[i + 1]}']

    if strict:
        return df[[f'madom{i}' for i in range(len(mas)-1)]].all(axis=1)
    else:
        return df[[f'madom{i}' for i in range(len(mas)-1)]].sum(axis=1)
