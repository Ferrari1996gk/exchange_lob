# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 20:31:09 2021

@author: kgao_smd
"""
import json
from datetime import timedelta
import numpy as np
import pandas as pd
from IPython.core.display import display
from analytics.trqx.turquoise_exchange import get_trades, get_orders, get_market_data
from numpy import genfromtxt

import matplotlib.pyplot as plt

cloud_path = "D:/Simudyne Limited/Data - LSE Market Data"
lit_path = '{0}/{1}'.format(cloud_path,  'L2/data-x.londonstockexchange.com/data-x/TRQX')
lob_path = '{0}/{1}'.format(cloud_path,  'LOB')

symbol = 'VODl'
mic = 'XLON'

tick_size = .02
step_size = 20000

#datestr = '2021-01-27'
#date = pd.to_datetime(datestr)
date_list = ['2021-01-29', '2021-01-28', '2021-01-27', '2021-01-26', '2021-01-25', 
             '2021-01-22', '2021-01-21', '2021-01-19', '2021-01-18']
tmp = []
for datestr in date_list:
    date = pd.to_datetime(datestr)
    trades = get_trades(lit_path, symbol, mic, date)
    # print(len(trades))
    new_trades = trades[trades['SIDE'] == 'Buy'].copy()
    print(len(new_trades))
    tmp += list(new_trades['EXECUTEDPRICE'])
    tmp += [0]


trades['trade_time'] = trades.index

new_trades = trades[trades['SIDE'] == 'Buy'].copy()
new_trades['min'] = new_trades['trade_time'].apply(lambda x: x.minute)
new_trades['hour'] = new_trades['trade_time'].apply(lambda x: x.hour)
tmp = []
for hour, df in new_trades.groupby('hour'):
    print(hour, ' ', len(df))
    tmp += list(df['EXECUTEDPRICE'])
    tmp += [0]

trade_price = new_trades['EXECUTEDPRICE']
trade_price.plot()
















