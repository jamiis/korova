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
import cbpro

# cb client
cb = cbpro.AuthenticatedClient(keys.apiKey, keys.apiSecret, keys.passphrase)

###
### iteratively acquire entire ETH-USD Coinbase market history
###
market = 'ETH-USD'
# start date of ETH-USD on Coinbase
start = dt(year=2017, month=5, day=18)
today = dt.today()
# start = dt(year=2016, month=5, day=18)
with PoolExecutor(max_workers=6) as executor:
    while True:
        ### do it multi-threaded (bursts of 6 requests per second allowed)
        get_hist = partial(cb.get_product_historic_rates, market)

        # list of tuple function args for multi-processing
        # each tuples is a time window
        time_windows =  [(start + timedelta(minutes=300*i), # start of window
                          start + timedelta(minutes=300*(i+1)), # end of window
                          60, # interval in seconds
                         ) for i in range(0,6)]

        res = executor.map(get_hist, *zip(*time_windows))

        import ipdb; ipdb.set_trace();
        # TODO store candle data in database
        # TODO time.sleep for some calculated amount
        # TODO output any request errors along with start datetime information

        for r in res:
            print(len(r))
            import ipdb; ipdb.set_trace();

        # run up until today's date
        if any([d['start'] > today for d in time_windows]):
            break
