# -*- coding: utf-8 -*-
"""
Download and clean GDAX historical data

@author: Mason
"""

import datetime as dt, pandas as pd, gdax, time, pickle

public_client = gdax.PublicClient()

date = dt.datetime.today()
dates = pd.date_range(end=date.isoformat(), periods=180, freq='1min') # Start in 2016H2
start_date = dates.min() - dt.timedelta(seconds=15) # The first date is not inclusive, so we need to start earlier by 15 seconds
end_date = dates.max()
rawdata = public_client.get_product_historic_rates('BTC-USD',start_date,end_date) # the default is 1-minute time-stamps. First entry is the most recent data point
rawdata.reverse()
btc = pd.DataFrame(rawdata,columns=['Unix Time','Low','High','Open','Close','Volume'])

# String data together starting from most recent. 
while True:
    curdate = dates.min() - dt.timedelta(minutes=1)
    dates = pd.date_range(end=curdate, periods=180, freq='1min') # Start in 2016H2
    start_date = dates.min() - dt.timedelta(seconds=15) # The first date is not inclusive, so we need to start earlier by 15 seconds
    end_date = dates.max()
    rawdata = public_client.get_product_historic_rates('BTC-USD',start_date,end_date) # the default is 1-minute time-stamps. First entry is the most recent data point
    
    if not rawdata or type(rawdata) is dict:
        time.sleep(3)
        continue
                         
    data = pd.DataFrame(rawdata,columns=['Unix Time','Low','High','Open','Close','Volume'])
    btc = pd.concat([btc,data])
    print(start_date)
                                              
datetimes = [pd.datetime.fromtimestamp(x) for x in btc['Unix Time']]
btc = btc.set_index([datetimes])

btc.isnull().sum() # No null data! We got everything we asked for!
                
btc['UsdOpen'] = btc['Volume']*btc['Open']
btc['UsdClose'] = btc['Volume']*btc['Close']

btc.resample('D').sum().resample('M').mean()['UsdOpen'].plot()

# There are some duplicates based on weird DST-type time issues. Keep the earliest date because it seems to stitch better.
btc = btc.sort_values('Unix Time')
btc = btc[~btc.index.duplicated()]

# Sort index, fill empty minute bars with ZOH.
btc = btc.sort_index().resample('min').pad()

# We shouldn't be ZOH the volume. So find the ZOH and replace with 0
btc_diff = btc.diff()
btc.loc[btc[btc_diff['Volume'] == 0].index,'Volume'] = 0 # NOTE THAT WE SHOULD NOT BE CHAINING HERE (e.g. btc['One']['Two'] versus btc.loc['One','Two']. SEE http://pandas.pydata.org/pandas-docs/stable/indexing.html#indexing-view-versus-copy
   
btc.to_excel('C:\\Users\\Mason\\Documents\\GitHub\\trading\\data\\btc_%s.xlsx' % date.strftime('%Y%m%d'),'wb')
               
pickle.dump(btc, open('C:\\Users\\Mason\\Documents\\GitHub\\trading\\data\\btc_%s.p' % date.strftime('%Y%m%d'),'wb'))
