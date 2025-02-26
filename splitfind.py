import pickle
from datafunc import *
from functions import *
import numpy as np
import os
from colorama import init, Fore, Style

db_uri = os.getenv('DB_URI_SHORT')

with open('recent_splits.pkl', 'rb') as file:
    splits = pickle.load(file)

jan = splits[splits['date'].between('2024-08-30', '2024-08-30')]
jantickers = jan['ticker'].unique()

cols = ['date', 'ticker', 'open', 'close', 'high', 'low', 'volume']
df = get_stocks(jantickers, from_date='2024-01-01', db_uri=db_uri, cols=cols) 
df['return'] = df.groupby('ticker')['close'].pct_change() + 1

print(f'{len(df.ticker.unique())} in df')
print(f'{len(jan.ticker.unique())} in jan')

splitprob = []
bad = []
questionmark = []
errors = 0
errortickers = []
error_messages = []
init()

for i, row in jan.iterrows():
    if row['ticker'] not in df.ticker.unique():
        continue

    try:
        ratio = float(row['from'] / row['to'])
        stockdf = df[df.ticker == row['ticker']].copy().reset_index()
        index = stockdf.loc[stockdf['date'] == row['date']].index[0]
        ret = float(stockdf.loc[stockdf.date == row['date']]['return'].iloc[0])
        grade, diff = validate_split(ratio, ret)
        splitprob.append(grade)

        if grade == 1:
            print(Fore.GREEN)
        elif grade == 0:
            print(Fore.RED)
            bad.append(row.ticker)
        else:
            print(Fore.YELLOW)
            questionmark.append(row.ticker)

        print('___________________________')
        print(f'\nRatio: {ratio} on {row.date}\nReturn: {ret}\nGrade: {grade}; Diff: {diff}')
        if grade == 1:
            print(stockdf.loc[index-1: index+1])
        else:
            try:
                print(stockdf.loc[index-5: index+5])
            except:
                print(stockdf.loc[index-1: index])

    except Exception as e:
        error_messages.append(e)
        errors += 1
        errortickers.append(row.ticker)
        splitprob.append(None)

print(Style.RESET_ALL)
print('\n')
print(f'{errors} errors')
print(f'{sum(np.array(splitprob) == 1)} certain splits')
print(f'{sum(np.array(splitprob) == 0)} certain nonsplits')
print(f'{np.sum([1 for item in splitprob if item is None])} aborts')
print(f'out of {jan.shape[0]} splits and {len(df.ticker.unique())} stocks')
print(f'bad tickers: {bad}')
print(f'None grade tickers: {questionmark}')
print(f'Encountered errors on tickers: {errortickers}')
print(error_messages)

