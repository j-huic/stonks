import pandas
import pickle
from datafunc import *
from functions import *
import numpy as np
import os
from colorama import init, Fore

db_uri = os.getenv('DB_URI_SHORT')

with open('recent_splits.pkl', 'rb') as file:
    splits = pickle.load(file)

jan = splits[splits['date'].between('2024-10-01', '2024-11-01')]
jantickers = jan['ticker'].unique()

df = get_stocks(jantickers, from_date='2024-01-01', db_uri=db_uri) 
df['return'] = df.groupby('ticker')['close'].pct_change() + 1

print(f'{len(df.ticker.unique())} in df')
print(f'{len(jan.ticker.unique())} in jan')

splitprob = []
errors = 0
init()

for i, row in jan.iterrows():
    if row['ticker'] not in df.ticker.unique():
        continue

    try:
        ratio = float(row['from'] / row['to'])
        stockdf = df[df.ticker == row['ticker']].copy().reset_index()
        index = stockdf.loc[stockdf['date'] == row['date']].index[0]
        ret = float(stockdf.loc[stockdf.date == row['date']]['return'].iloc[0])
        grade = validate_split(ratio, ret)
        splitprob.append(grade)

        if grade == 1:
            print(Fore.GREEN)
        elif grade == 0:
            print(Fore.RED)
        else:
            print(Fore.YELLOW)

        print('___________________________')
        print(f'\nRatio: {ratio} on {row.date}\nReturn: {ret}\nGrade: {grade}')
        print(stockdf.loc[index-1: index+1])
    except:
        errors += 1
        splitprob.append(None)

print('\n')
print(f'{errors} errors')
print(f'{sum(np.array(splitprob) == 1)} certain splits')
print(f'{sum(np.array(splitprob) == 0)} certain nonsplits')
print(f'{np.sum([1 for item in splitprob if item is None])} aborts')
print(f'out of {jan.shape[0]} splits and {len(df.ticker.unique())} stocks')

