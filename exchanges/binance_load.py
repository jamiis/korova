from functools import partial
from pprint import pprint
import multiprocessing as mp

import pandas as pd
import numpy as np
import ta
from bashplotlib.histogram import plot_hist

df_eth_btc = pd.read_csv('./data/BTC/ETH-BTC.csv', delimiter=',')
df_eth_btc['time'] = pd.to_datetime(df_eth_btc['open_time'], unit='ms')
df_eth_btc = df_eth_btc.set_index('time', drop=False)
df_eth_btc = df_eth_btc.drop_duplicates()

df_btc_usd = pd.read_csv('./data/USDT/BTC-USDT.csv', delimiter=',')
df_btc_usd['time'] = pd.to_datetime(df_btc_usd['open_time'], unit='ms')
df_btc_usd = df_btc_usd.set_index('time', drop=False)
df_btc_usd = df_btc_usd.drop_duplicates()

df_eth_usd = pd.read_csv('./data/USDT/ETH-USDT.csv', delimiter=',')
df_eth_usd['time'] = pd.to_datetime(df_eth_usd['open_time'], unit='ms')
df_eth_usd = df_eth_usd.set_index('time', drop=False)
df_eth_usd = df_eth_usd.drop_duplicates()

df = pd.read_csv('./data/USDT/ETH-USDT.csv', delimiter=',')
df['time'] = pd.to_datetime(df['open_time'], unit='ms')
df = df.set_index('time', drop=False)

# there are about 1000 duplicate timestamps with different OHLC values
df = df.drop_duplicates()

# TODO
# need robust volume smooth / removing / interpolating strategy
# maybe remove long sequences of 0.0 volume?
# maybe interpolate sequences of 5 minute low volume? 
# maybe remove rows that have long zero sequences but allow training on short zero sequences
# build tools to do all of the above so featurizer can be flexible
# run some basic analysis on differences between ETH-BTC, BTC-USDT, ETH-USDt
# TODO
# setup remote desktop? or figure out easy way to get plots from workstation


ts_lt = '2017-08-17 04:07:00'
ix_lt = 7
# find all occurences of consecutive volume=0.0
ts = '2019-11-13 04:20:00' # <-- this time has 4 vol=0.0 in a row
ix = 1171827
# 2019-11-13 01:57:00    95.16681
# 2019-11-13 01:58:00    32.70113
# 2019-11-13 01:59:00    15.39598
# 2019-11-13 04:20:00     0.00000
# 2019-11-13 04:21:00     0.00000
# 2019-11-13 04:22:00     0.00000
# 2019-11-13 04:23:00     0.00000
# 2019-11-13 04:24:00    33.04948

def get_consecutive_count(df):
    # find consecutive zeros that are greater than count
    vol = df.volume
    # cumulative sum of consecutive equal values of volume
    # TODO probably does not work well for date data
    counts = vol.groupby((vol != vol.shift()).cumsum()).transform('count')
    # only count where consecutive vol=0.0
    counts[vol != 0.0] = 0
    # count only once for sequences of consecutive vol=0.0
    mask = counts.astype(bool)
    mask &= mask.eq(True) & mask.shift().ne(True)
    counts[mask!=True] = 0
    return counts

counts = get_consecutive_count(df)
count = 60
occurences = counts.gt(count).astype(int).sum()
print(f'num of {count} consecutive candles with 0 volume: {occurences}')
yr_first = counts['2017':'2017-12-31'].astype(int).sum()
yr_other = counts['2018':'2021'].astype(int).sum()
print(f'occurences in 2017: {yr_first}')
print(f'occurences in after 2018: {yr_other}')
import ipdb; ipdb.set_trace();

onemin = pd.Timedelta(1, 'minutes')
indx = df[df.volume==0.0].volume.index
import ipdb; ipdb.set_trace();
vol = 10.0
acc = 0
for i in indx: 
    ii = df.index.get_loc(i)
    # TODO need to look at adjacent minute candle not adjacent index
    pre = df.iloc[ii-1].volume > vol
    nex = df.iloc[ii+1].volume > vol
    if pre or nex:
        acc += 1
        pprint(df.iloc[ii-5:ii+5].volume)
        # df[i-onemin:i+onemin]

print(f'acc: {acc}')
import ipdb; ipdb.set_trace();

# TODO we investigated the data health of ETH-BTC, check other pairs!!

# there are ~7k entries with volume = NaN
# TODO either fill or remove

import ipdb; ipdb.set_trace();
df = ta.utils.dropna(df)
print(df[df['volume'].isnull()].shape[0])
import ipdb; ipdb.set_trace();

# TODO interpolate volume=0.0 when consecutive for x=1-3 candles
# remove rows when consecutive missing data >=x 

# time = df.index.to_series()
df['delta'] = df.time - df.time.shift()
counts = df['delta'][1:].value_counts()
perc = 100*counts / counts.sum()
with pd.option_context('display.float_format', '{:0.5f}'.format):
    print(perc)

# get rows (and surrounding rows) where the delta between rows is > one minutes
onemin = pd.Timedelta(1, 'minutes')
oneday = pd.Timedelta(1, 'minutes')
indexes = np.unique(np.concatenate([(i-1,i,i+1) for i in np.where(df.delta>onemin)]))
gaps = df.iloc[indexes]

# break-up data where there are gaps in time / inconsistent data
dfs = np.split(df, *np.where(df.delta>onemin))
# filter out dataframes that don't cover at least 6 hours of time
dfs = [df for df in dfs if df.shape[0] > 6*60]
featurize = partial(ta.add_all_ta_features, open='open', high='high', low='low', close='close', volume='volume')
with mp.Pool(mp.cpu_count()) as pool:
    results = pool.map(featurize, dfs)
import ipdb; ipdb.set_trace();

# IPython CPU timings (estimated):
# User   :       4.09 s.
# System :       3.20 s.
# Wall time:     466.08 s.
"""
fillna = False
fns = [partial(ta.add_volume_ta, df, 'high', 'low', 'close', 'volume', fillna),
       partial(ta.add_volatility_ta, df, 'high', 'low', 'close', fillna),
       partial(ta.add_trend_ta, df, 'high', 'low', 'close', fillna),
       partial(ta.add_momentum_ta, df, 'high', 'low', 'close', 'volume', fillna),
       partial(ta.add_others_ta, df, 'close', fillna)]
def _run(fn): return fn()
with mp.Pool(mp.cpu_count()) as pool:
    results = pool.map(_run, fns)
"""
# takes a very long time to compute
# dfta = ta.add_all_ta_features(df, "open", "high", "low", "close", "volume")
