__author__ = "Krishnen Vytelingum"
__copyright__ = "Copyright 2021, Simudyne"
__version__ = "0.1"
__maintainer__ = "Krishnen Vytelingum"
__email__ = "krishnen@simudyne.com"
__status__ = "Prototype"

from typing import Union
from pandas import DatetimeIndex
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from exception.SimudyneException import ParserException

"""
Description: Set of functions to formatted extract Turquoise market data from CSV data files.
"""

date_format = "%Y%m%d"
turquoise_datetime_format = "%d-%b-%y %H.%M.%S.%f"


def get_datetime(ts, hour_offset: int = 0) -> Union[datetime, DatetimeIndex]:
    """Return a datetime given a string, given the turquoise format.

    :param ts: time input as a string
    :param hour_offset: number of hours to offset the output (default 0)
    :return: datetime
    """
    return pd.to_datetime(ts, format=turquoise_datetime_format) + timedelta(hours=hour_offset)


def get_date_str(date: datetime, format=date_format) -> str:
    """Return a string of a datetime, given the turquoise format.

    :param format: format of datetime
    :param date: time input as a datetime
    :return: datetime string
    """
    return date.strftime(format)


def get_open(date: datetime) -> datetime:
    """Return the OPEN of the Turquoise market just before 16:30:00.000000.

    :param date: time input as a datetime
    :return: market open datetime
    """
    return pd.to_datetime('{0} 08:00:00.000000'.format(get_date_str(date)))


def get_close(date: datetime) -> datetime:
    """Return the CLOSE of the Turquoise market just before 16:30:00.000000.

    :param date: time input as a datetime
    :return: market close datetime
    """
    return pd.to_datetime('{0} 16:29:59.999999'.format(get_date_str(date)))


def get_dataframe_with_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with a datetime index and adjust index for British Summer Time (BST).

    :param df: dataframe with an index of string dates
    :return: panda dataframe a datetime index
    """
    if df.shape[0] == 0:
        print('ERROR')
        raise ParserException

    date = get_datetime(df.index[0])
    dt = 8 - date.hour
    df.index = get_datetime(df.index, dt)

    close = get_close(date)

    return df.sort_index().loc[:close]


def _read_data(dataset_name: str, path: str, symbol: str, mic: str, date: datetime) -> pd.DataFrame:
    """Return a dataframe from raw Turquoise CSV file with TRANSACTTIME as index.

    :param dataset_name: name of dataset to read
    :param path: path of dataset
    :param symbol: symbol to read
    :param mic: venue to read
    :param date: date to read
    :return: panda Dataframe of CSV file
    """
    return pd.read_csv('{0}/{1}/{2}/{2}_{3}_{4}_{5}.csv'.format(path, dataset_name, symbol, mic, get_date_str(date),
                                                                dataset_name)).set_index('TRANSACTTIME')


def get_orders(path: str, symbol: str, mic: str, date: datetime) -> pd.DataFrame:
    """Return Turquoise LitOrders dataset as a dataframe with a datetime index.

    :param path -- path of dataset
    :param symbol -- symbol to read
    :param mic -- venue to read
    :param date -- date to read
    :return: panda Series of orders
    """
    raw_orders = _read_data('LitOrders', path, symbol, mic, date)

    return get_dataframe_with_datetime(raw_orders)


def get_trades(path: str, symbol: str, mic: str, date: datetime) -> pd.DataFrame:
    """Return Turquoise LitTrades as a dataframe with a datetime index.

    :param path: path of dataset
    :param symbol: symbol to read
    :param mic: venue to read
    :param date: date to read
    :return: panda DataFrame of trades
    """
    raw_trades = _read_data('LitTrades', path, symbol, mic, date)

    return get_dataframe_with_datetime(raw_trades)


def _read_lob_data(dataset_name: str, path: str, symbol: str, mic: str, date: datetime) -> pd.DataFrame:
    """Read a CSV file of rebuilt limit orderbook data.

    :param dataset_name: name of dataset to read
    :param path: path of dataset
    :param symbol: symbol to read
    :param mic: venue to read
    :param date: date to read
    :return: panda DataFrame of LOB data
    """
    date_str = get_date_str(date)
    df = pd.read_csv('{0}/{1}/{2}_{3}_{4}_{5}.csv'.format(path, symbol, symbol, mic, date_str, dataset_name))

    return df


def get_lob_data(dataset_names, path, symbol, mic, date) -> dict:
    """
    Return LOB data of rebuilt limit orderbook.
    The following datasets can be selected: 'orders', 'trades', 'lob_l1', 'lob_l2', 'lob', 'orders_cancelled'.

    :param dataset_names: names of dataset to read.
    :param path: path of dataset
    :param symbol: symbol to read
    :param mic: venue to read
    :param date: date to read
    :return: dict of LOB market data
    """
    df_lob = {}
    for name in dataset_names:
        df = _read_lob_data(name, path, symbol, mic, date).set_index('time')
        df_lob[name] = get_dataframe_with_datetime(df)

    return df_lob


def get_bbo(l1: pd.DataFrame, side: str, shift: int = 0) -> pd.DataFrame:
    """Return the shifted best bid or ask/offer (BBO).

    :param l1: L1 dataset
    :param side: buy ('B') or sell ('S') side
    :param shift: shift in datetime index (default 0)
    :return: a panda Series of best bid or offer
    """
    bbo = l1[l1['side'] == side]['prc'].shift(shift)

    return bbo[~bbo.index.duplicated(keep='last')]


def get_prc_in_tick(prc: pd.Series, tick_size: float) -> pd.Series:
    """Return prices in ticks.

    :param prc: market price of a symbol as a panda Series
    :param tick_size: symbol tick size
    :return: pandas Series with prices in ticks.
    """
    return np.round(prc / tick_size).fillna(method='ffill')


def get_market_data(lit_path: str, lob_path: str, symbol: str, mic: str, date: datetime, tick_size: float,
                    include_l2: bool = False) -> dict:
    """Get Turquoise LitOrders and LitTrades.

    :param lit_path: path of LitOrders and LitTrades
    :param lob_path: path of dataset
    :param symbol: symbol to read
    :param mic: venue to read
    :param date: date to read
    :param tick_size: symbol tick size
    :param include_l2: bool flag to output L2 or not (default False)
    :return: dict of market data
    """
    data = get_lob_data(['lob_l1', 'lob_l2'] if include_l2 else ['lob_l1'], lob_path, symbol, mic, date)

    # Drop unused Lit fields
    orders = get_orders(lit_path, symbol, mic, date).iloc[:, 4:-2].drop(['ORDERBOOK', 'ISIN', 'CURRENCY', 'MIC'],
                                                                        axis=1)
    trades = get_trades(lit_path, symbol, mic, date).iloc[:, 4:]

    # Get BBO shifted by 1 tick to correlate to order decision marking
    b0 = get_bbo(data['lob_l1'], 'B', shift=1)
    a0 = get_bbo(data['lob_l1'], 'S', shift=1)

    # Extend orders dataframe with BBO and spread columns
    orders['b0'] = b0.reindex(orders.index)
    orders['a0'] = a0.reindex(orders.index)
    orders['spread'] = get_prc_in_tick(orders['a0'] - orders['b0'], tick_size)

    # Get distance of each order to the cross touch
    dpx_sell = orders['PRICE'] - orders['b0']
    dpx_buy = orders['a0'] - orders['PRICE']
    dpx = (orders['SIDE'] == 'Sell') * dpx_sell + (orders['SIDE'] == 'Buy') * dpx_buy

    # Get distance of each order to the touch
    dp_sell = orders['PRICE'] - orders['a0']
    dp_buy = orders['b0'] - orders['PRICE']
    dp = (orders['SIDE'] == 'Sell') * dp_sell + (orders['SIDE'] == 'Buy') * dp_buy

    # Extend orders dataframe with depth and cross-touch depth (depth from other side)
    orders['dpx'] = get_prc_in_tick(dpx, tick_size)
    orders['dp'] = get_prc_in_tick(dp, tick_size)

    # Infer market orders as orders recorded in LitTrades, but not in LitOrders as they aggressively clear the LOB
    market_orders = trades[~trades['PUBLICORDERID'].isin(orders['PUBLIC_ORDER_ID'].unique())]

    market_data = {'l1': data['lob_l1'], 'orders': orders, 'trades': trades, 'market_orders': market_orders,
                   'a0': a0, 'b0': b0}

    if include_l2:
        market_data['l2'] = data['lob_l2']

    return market_data
