import pandas
import pickle
from datafunc import *
from functions import *
import numpy as np
import os

db_uri = os.getenv('DB_URI_SHORT')

with open('recent_splits.pkl', 'rb') as file:
    splits = pickle.load(file)

jan = splits[splits['date'].between('2024-01-01', '2024-02-01')]
print(jan.head())
jantickers = jan['ticker'].unique()

df = get_stocks(jantickers, from_date='2024-01-01', db_uri=db_uri) 
print(df.head())
df['return'] = (df['close'].pct_change() / 100) + 1

print(f'{len(df.ticker.unique())} in df')
print(f'{len(jan.ticker.unique())} in jan')

splitprob = []

for i, row in jan.iterrows():
    if row['ticker'] not in df.ticker.unique():
        continue
    ratio = float(row['from'] / row['to'])
    stockdf = df[df.ticker == row['ticker']].copy()
    ret = float(stockdf.loc[stockdf.date == row['date']]['close'].iloc[0])
    splitprob.append(validate_split(ratio, ret))

print('\n')
print(f'{np.sum(splitprob == 1)} certain splits')
print(f'{np.sum(splitprob == 0)} certain nonsplits')
print(f'out of {jan.shape[0]} splits')

