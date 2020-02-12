# python libs
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from datetime import datetime as dt
from datetime import timedelta
from functools import partial
import time
from pprint import pprint

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



def _get_data(market, start, end, granularity, attempts=10, sleep=2):
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
    def _is_rate_limited(res):
        return isinstance(res, dict) and 'rate limit' in res['message'].lower()

    def _sleep_on_rate_limit(res, multiplier=1):
        seconds = sleep*multiplier
        print('\nerror :{0:s}\n--- sleeping {1:} seconds ---\n'.format(res['message'], seconds))
        time.sleep(seconds)

    def _retrieve_data():
        return cb.get_product_historic_rates(market, start, end, granularity)

    res = _retrieve_data()
    # if rate limited, sleep and try to retrieve data again and again
    for count, _ in enumerate(range(0, attempts)):
        if not _is_rate_limited(res):
            break
        _sleep_on_rate_limit(res, count+1)
        res = _retrieve_data()
    res.reverse()
    return res



if __name__ == "__main__":
    exec_time = time.time()

    start_date = dt(year=2017, month=6, day=18)
    dates = _gen_date_frames(start_date)

    # TODO make pystore vars as program args
    store = pystore.store('store_')
    collection = store.collection('coll_')

    for count, (start, end) in enumerate(dates):
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
        if count == 0: # first run, create data store
            # TODO what metadata do we want?
            collection.write('ethusd_', df, metadata={'granularity':granularity})
        else: # all other runs, append data
            collection.append('ethusd_', df, npartitions=1)


        # LOGGING
        # TODO setup logging file
        print('count', count)
        print('start', start)
        print('end  ', end)
        print('res len:', len(res))

        elapsed_time = time.time() - exec_time
        avg_exec_time =  elapsed_time / (count+1.0)
        print("avg secs/req {0:.4f}".format(avg_exec_time))
        print()

