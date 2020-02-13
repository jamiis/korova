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
import keys

# external libs
import numpy as np
import pandas as pd
import cbpro
import pystore
pystore.set_path('./pystore')

# cb client
# cb = cbpro.AuthenticatedClient(keys.apiKey, keys.apiSecret, keys.passphrase)
cb = cbpro.PublicClient()

###
### iteratively acquire entire ETH-USD Coinbase market history
###
market = 'ETH-USD'

# ready function to retrieve data multi-threaded
get_hist = partial(cb.get_product_historic_rates, market)

# start date of ETH-USD on Coinbase
# start = dt(year=2017, month=5, day=18) NOTE: the real start date
# start = dt(year=2017, month=6, day=18)
today = dt.today()
# start = dt(year=2016, month=5, day=18)

frames = 3 # number of frames fetched asynchronously
width = 300 # number of results per time frame
granularity = 60 # avail: 60 (min), 300 (5min), 900 (15min), 3600 (1hr), 21600 (6hr), 86400 (1day)

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
        start = end + timedelta(minutes=width)
        end = start + timedelta(minutes=width)



def _get_data(market, start, end, granularity, attempts=15, sleep=1):
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
        logger.warning('rate limited')
        logger.warning('response: %s', res)
        logger.warning('sleeping %s seconds', seconds)
        time.sleep(seconds)

    def retrieve_data():
        logger.info('request data. mkt: %s, start: %s, end: %s, granularity: %s', 
                market, start, end, granularity)
        return cb.get_product_historic_rates(market, start, end, granularity)

    res = retrieve_data()
    # if rate limited, sleep and try to retrieve data again and again
    for count, _ in enumerate(range(0, attempts)):
        if not is_rate_limited(res):
            break
        sleep_on_rate_limit(res, additional=count)
        res = retrieve_data()
    res.reverse()

    logger.info('request successful. num candles: %s', len(res))
    return res



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="retrieve and store historic candle data from Coinbase API")
    loglevels = { 'debug': logging.DEBUG,
                  'info': logging.INFO,
                  'warning': logging.WARNING,
                  'error': logging.ERROR,
                  'critical': logging.CRITICAL}
    parser.add_argument('--loglevel', type=str, 
            choices=loglevels.keys(), 
            default='info', 
            help='logger level')
    parser.add_argument('--market', type=str, 
            default='ETH-USD',
            help='Coinbase API market ticker')
    args = parser.parse_args()

    # use formatted datetime for filenames
    curr_datetime_str = dt.today().strftime('%Y-%m-%d-%H-%M-%S')

    # setup logging
    # TODO change to __name__ once you've made __init__.py
    _name = 'historic.cb'
    config = {
            # 'disable_existing_loggers': False,
            'version': 1,
            'formatters': {
                'short': {
                    'format': '%(asctime)s %(levelname)s: %(message)s'
                    },
                },
            'handlers': {
                'file': {
                    'level': loglevels[args.loglevel],
                    'formatter': 'short',
                    'class': 'logging.FileHandler',
                    'filename': 'logs/{:s}'.format(market),
                    },
                'console': {
                    'level': loglevels[args.loglevel],
                    'formatter': 'short',
                    'class': 'logging.StreamHandler',
                    },
                },
            'loggers': {
                _name: {
                    'handlers': ['file','console'],
                    'level': loglevels[args.loglevel],
                    },
                },
            }
    import logging.config
    logging.config.dictConfig(config)
    logger = logging.getLogger(_name)
    logger.info('=== new run: {:s} ==='.format(curr_datetime_str))

    # setup pystore for storing time series data
    ps_store = pystore.store('coinbase')
    ps_collection = ps_store.collection('historic.candles')
    ps_item = '{:s}-{:s}'.format(market, curr_datetime_str)

    # track execution time to monitor avg request time
    exec_time = time.time()

    # TODO start date needs to come from a file or argparse
    #      this start date is for ETH-USD
    start_date = dt(year=2017, month=6, day=18)
    dates = _gen_date_frames(start_date)

    for index, (start, end) in enumerate(dates):
        # CB API limited to 3 reqs/sec
        time.sleep(0.1)

        # retrieve data from CB API
        res = _get_data(market, start, end, granularity)

        # python -> pandas time-series dataframe
        df = pd.DataFrame(res, columns=['unixtime', 'low', 'high', 'open', 'close', 'volume'])
        df['unixtime'] = pd.to_datetime(df['unixtime'], unit='s')
        df = df.set_index('unixtime')

        # TODO confirm this data structure is the one we want to be storing.
        #      mostly, indexing by pd.DateTime object

        # write dataframe to data store
        if index == 0: # first run, create data store
            # TODO what metadata do we want?
            meta = { 'market': market, 'granularity': granularity}
            ps_collection.write(ps_item, df, metadata=meta)
        else: # all other runs, append data
            ps_collection.append(ps_item, df, npartitions=1)
        logger.info('dataframe stored. first row: timestamp: %s, data: %s', 
                df.index[0], df.iloc[0].to_dict())

        # log average seconds per request
        # NOTE: Coinbase limits to 3 per second but it's far from exact
        elapsed_time = time.time() - exec_time
        avg_exec_time =  elapsed_time / (index+1.0)
        logger.info("avg secs/req {0:.4f}".format(avg_exec_time))

