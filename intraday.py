import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pickle
from datafunc import *

from importlib import reload
import classes
reload(classes)


with open('spvix.pkl', 'rb') as file:
    df = pickle.load(file)

sp = df[df.ticker == 'SPY'][['date', 'price']].copy()
uvix = df[df.ticker == 'UVIX'][['date', 'price']].copy()
n = pd.merge(left=sp, right=uvix, on='date', how='inner')
n.columns = ['date', 'spy', 'uvix']
ytd = n[n.date > '2025-01-01 00:00'].reset_index().copy()

# b = classes.Backtest(stock_a=n.spy, stock_b=n.uvix, threshold=[33, 34])


def condition_dur(vector, n):
    roll_sum = vector.rolling(n).sum()
    return roll_sum >= n


plt_bicolor(ytd.uvix, ytd.uvix<30)

test = df_to_strat_ret(ytd, 28, 34, weights=(0.7,0.3))
plt.plot(np.cumprod(test))
test



ytd['incondition'] = ytd['uvix'] < 28
ytd['outcondition'] = ytd['uvix'] > 32

spret = ytd.spy.pct_change() + 1
vixret = ytd.uvix.pct_change() + 1
pos = strat(ytd, 4)

bla = ret_from_pos(spret, vixret, pos)
np.cumprod(bla)

plt.plot(np.cumprod(bla))


spweights = [0.9 if item else 1 for item in pos]
vixweights = [0.1 if item else 0 for item in pos]
sptotret = spweights * spret
vixtotret = vixweights * vixret
totret = sptotret + vixtotret


spret
sptotret

np.cumprod(totret).tail()
totret



# spret = np.array([1.05, 1.03, 1.1])
# vixret = np.array([1.02, 1.07, 1.2])
# spw = np.array([0.9, 0.9, 1])
# vw = np.array([0.1, 0.1, 0])
#
# sptotret = spw * spret
# vtotret = vw * vixret
# totret = sptotret + vtotret






a = [1.05, 1.03, 1.1]
b = [1, 0, 1]

np.cumprod([1.1,1.2,1.1])

    

#
ytd.head()

mask = strat(ytd, 4)

plt_bicolor(ytd.uvix, np.array(mask))

sum(mask)
mask
ytd['condition'] = ytd['uvix'] < 34
ytd.iloc[:5]

strat(ytd.iloc[:5], 4)


a = [0, 1, 2, 3, 4]

a[4:5]
