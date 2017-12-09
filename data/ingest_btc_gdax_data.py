# -*- coding: utf-8 -*-
"""
Download and clean GDAX historical data

@author: Mason
"""

import datetime as dt, pandas as pd, numpy as np
import gdax, time, pickle, PyQt5, pypyodbc, os

public_client = gdax.PublicClient()

date = dt.datetime.today()
dates = pd.date_range(end=date.isoformat(), periods=180, freq='1min') # Start in 2016H2
start_date = dates.min() - dt.timedelta(seconds=15) # The first date is not inclusive, so we need to start earlier by 15 seconds
end_date = dates.max()
rawdata = public_client.get_product_historic_rates('BTC-USD', start_date, end_date) # the default is 1-minute time-stamps. First entry is the most recent data point
rawdata.reverse() # This does the operation IN-PLACE
btc = pd.DataFrame(rawdata, columns=['Unix Time', 'Low', 'High', 'Open', 'Close', 'Volume'])

# String data together starting from most recent. Run this for 8*30 = 1 month
counter = 0
while counter < 8*30:
    counter = counter + 1
    curdate = dates.min() - dt.timedelta(minutes=1)
    dates = pd.date_range(end=curdate, periods=180, freq='1min') # Start in 2016H2
    start_date = dates.min() - dt.timedelta(seconds=15) # The first date is not inclusive, so we need to start earlier by 15 seconds
    end_date = dates.max()
    rawdata = public_client.get_product_historic_rates('BTC-USD',start_date,end_date) # the default is 1-minute time-stamps. First entry is the most recent data point
    
    if not rawdata or type(rawdata) is dict:
        time.sleep(3)
        continue
                         
    data = pd.DataFrame(rawdata, columns=['Unix Time', 'Low', 'High', 'Open', 'Close', 'Volume'])
    btc = pd.concat([btc, data])
    print(start_date)
                                              
datetimes = [pd.datetime.fromtimestamp(x) for x in btc['Unix Time']]
btc = btc.set_index([datetimes])

assert btc.isnull().sum().sum() == 0, 'We got some missing data...'

btc['UsdOpen'] = btc['Volume']*btc['Open']
btc['UsdClose'] = btc['Volume']*btc['Close']

btc_daily_sum = btc.resample('D').sum()
btc_monthly_mean = btc_daily_sum.resample('M').mean()
# btc_monthly_mean['UsdOpen'].plot()

# There are some duplicates based on weird DST-type time issues. Keep the earliest date because it seems to stitch better.
btc = btc.sort_values('Unix Time')
btc = btc[~btc.index.duplicated()]

# Sort index, fill empty minute bars with ZOH.
btc = btc.sort_index().resample('min').pad()

# We shouldn't be ZOH the volume. So find the ZOH and replace with 0

btc_diff = btc.diff()
btc.loc[btc_diff['Volume'] == 0, 'Volume'] = 0

path_to_file = os.getcwd() + '\\temp\\btc.csv'
btc.to_csv(path_to_file)

server = 'mason.database.windows.net'
database = 'btc'

auth = 'ActiveDirectoryPassword'
driver= '{ODBC Driver 13 for SQL Server}'

conn_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1443;AUTHENTICATION='+auth+';DATABASE='+database+';UID='+username+';PWD='+ password
cnxn = pypyodbc.connect(conn_str)
cursor = cnxn.cursor()

sql = 'BULK INSERT btc.dbo.historical_minute_bars FROM {}'.format(path_to_file)

insert_list = []
for idx in btc.index:
    insert_list.append((idx, int(btc.loc[idx, 'Unix Time']), btc.loc[idx, 'Low'], btc.loc[idx, 'High'],
                        btc.loc[idx, 'Open'], btc.loc[idx, 'Close'], btc.loc[idx, 'Volume'], btc.loc[idx, 'UsdOpen'],
                        btc.loc[idx, 'UsdClose']))

sql = 'INSERT INTO btc.dbo.historical_minute_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
cursor.executemany(sql, insert_list)

    print('Currently on: ' + idx.strftime('%Y-%m-%d %H:%M:%S'))
    cursor.execute('INSERT INTO btc.dbo.historical_minute_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                   idx, int(btc.loc[idx, 'Unix Time']), btc.loc[idx, 'Low'], btc.loc[idx, 'High'],
                   btc.loc[idx, 'Open'], btc.loc[idx, 'Close'], btc.loc[idx, 'Volume'], btc.loc[idx, 'UsdOpen'], btc.loc[idx, 'UsdClose'])
