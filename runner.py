import pandas as pd
from data import apis
import datetime as dt

date_ranges = pd.date_range(start='20150201', end=dt.datetime.now().strftime('%Y%m%d'))
for cur_date in date_ranges:
    prev_date = cur_date - dt.timedelta(days=1)
    dates = pd.date_range(start=prev_date.strftime('%Y%m%d'), end=cur_date.strftime('%Y%m%d'), freq='3H')
    apis.download_gdax_data(dates)


