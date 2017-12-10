# -*- coding: utf-8 -*-
"""
Download and clean GDAX historical data

@author: Mason
"""

import datetime as dt, pandas as pd
import gdax, time, PyQt5, pyodbc, os, sqlalchemy, urllib

public_client = gdax.PublicClient()

dates = pd.date_range(start='20150101', end='20150201', freq='3H')

btc = pd.DataFrame()
# This will run all the data up to, but no including, today
for cur_date in dates[:-1]:
    counter = 0
    print(cur_date)
    start_date = cur_date - dt.timedelta(seconds=15) # The first date is not inclusive, so we need to start earlier by 15 seconds
    end_date = cur_date + dt.timedelta(hours=3)
    rawdata = public_client.get_product_historic_rates('BTC-USD', start_date, end_date)  # the default is 1-minute time-stamps. First entry is the most recent data point

    while not rawdata or type(rawdata) is dict:
        time.sleep(3)
        rawdata = public_client.get_product_historic_rates('BTC-USD', start_date, end_date)
        counter = counter + 1
        if counter > 2:
            print('Giving up on ' + cur_date.strftime('%Y-%m-%d %H:%M:%S'))
            break

    data = pd.DataFrame(rawdata, columns=['timestamp', 'low', 'high', 'po', 'pc', 'volume'])
    btc = pd.concat([btc, data])

btc['timestamp'] = [pd.datetime.fromtimestamp(x) for x in btc['timestamp']]
btc = btc.set_index('timestamp')

assert btc.isnull().sum().sum() == 0, 'We got some missing data...'

# btc_daily_sum = btc.resample('D').sum()
# btc_monthly_mean = btc_daily_sum.resample('M').mean()

# There are some duplicates based on weird DST-type time issues. Keep the earliest date because it seems to stitch better.
btc = btc.sort_index()
btc = btc[~btc.index.duplicated()]

# Sort index, fill empty minute bars with ZOH.
btc = btc.sort_index().resample('min').pad()

# We shouldn't be ZOH the volume. So find the ZOH and replace with 0
btc_diff = btc.diff()
btc.loc[btc_diff['volume'] == 0, 'volume'] = 0

# params
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

start = time.time()
btc.to_sql('hist_minute_bars', engine, if_exists='append', index_label='timestamp')
print('Finished ' + str(len(btc)) + ' rows in : ' + str(time.time() - start))