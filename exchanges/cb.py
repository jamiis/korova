# python libs
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from datetime import datetime as dt
from datetime import date
from datetime import timedelta
from functools import partial
import time
from pprint import pprint
import logging
import argparse

# project libs
# import keys
import logger

# external libs
import numpy as np
import pandas as pd
import cbpro
cb = cbpro.PublicClient()
# cb = cbpro.AuthenticatedClient(keys.apiKey, keys.apiSecret, keys.passphrase)
import pystore
pystore.set_path('./pystore')



def get_start_date(market):
    return {'ETH-USD': dt(year=2016, month=6, day=17),
            'BTC-USD': dt(year=2015, month=7, day=21)
            }[market]



def _gen_date_frames(start, until=None, width=300):
    """Generator for list of sequential time-window tuples.

    Args:
        start (datetime): Start datetime of list of dates
        until (datetime): Generate list until this date is hit
        width (int): Width of each time frame in minutes
    """
    end = start + timedelta(minutes=width)
    while start <= until if until else dt.today():
        yield start, end
        start = end
        end = start + timedelta(minutes=width)



def get_data(market, start, end, granularity, attempts=15, sleep=1):
    """Retrieve historical data from Coinbase API.
    Attempts resiliency to rate limiting by delaying 
    new requests after halted.

    Args:
        start (datetime): Start datetime of historical data window.
        end (datetime): End datetime of historical data window.
        granularity (int): Candle granularity in seconds. 
            options: 60 (min), 300 (5min), 900 (15min), 3600 (1hr), 
            21600 (6hr), 86400 (1day)
        attempts (int): Number of attempts made after rate limiting.
        sleep (int): Sleep in seconds after rate limiting. 
            Increases by one multiple each time.
    Returns:
        list: List of candle data from Coinbse corresponding to `start` and `end`.
    """
    def is_rate_limited(res):
        return isinstance(res, dict) and 'rate limit' in res['message'].lower()

    def sleep_on_rate_limit(res, additional=0):
        seconds = sleep + additional
        log.warning('rate limited')
        log.warning('response: %s', res)
        log.warning('sleeping %s seconds', seconds)
        time.sleep(seconds)

    def retrieve_data():
        log.info('request data. mkt: %s, start: %s, end: %s, granularity: %s', 
                market, start, end, granularity)
        return cb.get_product_historic_rates(market, start, end, granularity)

    res = retrieve_data()
    # if rate limited, sleep and try to retrieve data again and again
    for count, _ in enumerate(range(0, attempts)):
        if not is_rate_limited(res):
            break
        sleep_on_rate_limit(res, additional=count)
        res = retrieve_data()
    log.info('request successful')
    res.reverse()
    return res



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="retrieve and store historic candle data from Coinbase API")
    parser.add_argument('--loglevel', type=str, 
            choices=logger.levels.keys(), 
            default='info', 
            help='log level')
    parser.add_argument('--market', type=str, 
            default='ETH-USD',
            help='Coinbase API market ticker')
    parser.add_argument('--granularity', type=int,
            default=60,
            choices=[60, 300, 900, 3600, 21600, 86400],
            help='granularity of candle data in seconds. choices: 1m, 5m, 15m, 6h, 24h')
    args = parser.parse_args()

    # use formatted datetime for filenames
    curr_datetime_str = dt.today().strftime('%Y-%m-%d-%H-%M-%S')

    # setup logging
    log_name = 'exchanges.cb'
    log_filename = 'logs/{:s}'.format(args.market)
    log = logger.setup(log_name, log_filename, logger.levels[args.loglevel])
    log.info('=== new run: {:s} ==='.format(curr_datetime_str))

    # setup pystore for storing time series data
    ps_store = pystore.store('coinbase')
    ps_collection = ps_store.collection('candles.minute')
    ps_item = '{:s}-{:s}'.format(args.market, curr_datetime_str)

    # track execution time to monitor avg request time
    exec_time = time.time()

    start_date = get_start_date(args.market)
    dates = _gen_date_frames(start_date)

    for index, (start, end) in enumerate(dates):
        # CB API limited to 3 reqs/sec but it's not accurate at all
        time.sleep(0.2)

        # retrieve data from CB API
        res = get_data(args.market, start, end, args.granularity)

        # python -> pandas time-series dataframe
        df = pd.DataFrame(res, columns=['unixtime', 'low', 'high', 'open', 'close', 'volume'])
        df['unixtime'] = pd.to_datetime(df['unixtime'], unit='s')
        df = df.set_index('unixtime')

        # TODO confirm this data structure is the one we want to be storing.
        #      mostly, indexing by pd.DateTime object

        # write dataframe to data store
        if index == 0: # first run, create data store
            meta = { 'market': args.market, 'granularity': args.granularity}
            ps_collection.write(ps_item, df, metadata=meta)
        else: # all other runs, append data
            ps_collection.append(ps_item, df, npartitions=1)
        log.info('dataframe stored. num rows: %s', df.shape[0])
        log.info('first row: timestamp: %s, data: %s', 
                df.index[0], df.iloc[0].to_dict())

        # log average seconds per request
        # NOTE: Coinbase limits to 3 per second but it's far from exact
        elapsed_time = time.time() - exec_time
        avg_exec_time =  elapsed_time / (index+1.0)
        log.info("avg secs/req {0:.4f}".format(avg_exec_time))
