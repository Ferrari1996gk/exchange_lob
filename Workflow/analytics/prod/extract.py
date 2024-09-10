__author__ = "Krishnen Vytelingum"
__copyright__ = "Copyright 2021, Simudyne"
__version__ = "0.1"
__maintainer__ = "Krishnen Vytelingum"
__email__ = "krishnen@simudyne.com"
__status__ = "Prototype"

import zipfile
import pandas as pd

import exception.SimudyneException as SimudyneException

from datetime import datetime
from os import listdir
from os.path import isfile, join

import pyarrow.parquet as pq


def _extract_ext_trader_timeseries(path, name):
    _path = _get_complete_filepath('{0}/ext-trader'.format(path), name)

    index_proxy = {'market_data': 'time_utc', 'order_data': 'real_txn_time', 'trade_data': 'time_utc'}
    df = pd.read_parquet('{0}'.format(_path)).set_index(index_proxy[name])
    try:
        ii = pd.to_datetime(df.index.str.decode("utf-8"), format='%Y%m%d%H%M%S.%f')
        df.index = ii
        df = df.sort_index()
    except TypeError as e:
        print('MDGW: Unable to get datetime for {0}; {1} ({2}).'.format(path, name, e))
        pass

    return df


def _extract_ext_trader(path):
    """Extract market data for external trader."""
    return {name: _extract_ext_trader_timeseries(path, name) for name in ['market_data', 'order_data', 'trade_data']}


def _get_complete_filepath(path: str, name: str) -> str:
    """Get the full filename given a partial name."""
    fs = [f for f in listdir(path) if isfile(join(path, f)) and name in f]

    if len(fs) != 1:
        raise SimudyneException('File missing')

    return '{0}/{1}'.format(path, fs[0])


def _extract_mdgw_timeseries(path, name):
    _path = _get_complete_filepath('{0}/mdgw'.format(path), name)
    df = pd.read_csv(_path).set_index('tstamp')
    try:
        ii = pd.to_datetime(df.index, format='%Y%m%d-%H:%M:%S.%f')
        df.index = ii
        df = df.sort_index()
    except TypeError as e:
        print('MDGW: Unable to get datetime for {0}; {1} ({2}).'.format(path, name, e))
        pass

    return df


def _extract_mdgw(path):
    """Extract market data gatewoy data."""
    return {name: _extract_mdgw_timeseries(path, name) for name in ['instruments', 'market_data', 'orders', 'stats',
                                                                    'trades']}


def _extract_mktsim_timeseries(path):
    df = pq.read_table(path).to_pandas()
    try:
        if 'time_utc' in df.columns or 'real_txn_time' in df.columns:
            df = pq.read_table(path).to_pandas().set_index('time_utc' if 'time_utc' in df.columns else 'real_txn_time')
            ii = pd.to_datetime(df.index.str.decode("utf-8"), format='%Y%m%d%H%M%S.%f')
            df.index = ii
            df = df.sort_index()

    except Exception:
        print('MKTSIM: Unable to get datetime for {0}.'.format(path))
        pass

    return df


def _extract_mktsim(date_path, date: datetime):
    """Extract data from the market simulator."""
    dct = {name: _extract_mktsim_timeseries(_get_complete_filepath('{0}/mktsim'.format(date_path), name))
           for name in ['market', 'trade']}

    path = '{0}/mktsim'.format(date_path)
    order_paths = [f for f in listdir(path) if isfile(join(path, f)) and 'order_data' in f]
    dct['order'] = pd.concat([_extract_mktsim_timeseries(join(path, _path)) for _path in order_paths]).sort_index()

    return dct


def get_data(user_path, date_str, run_id=None):
    """Extract all data to analyse PROD output."""

    date = pd.to_datetime(date_str)
    date_str = date.strftime('%Y-%m-%d')
    _folder_name = date_str if run_id is None else '{0}/{1}'.format(date_str, run_id)
    path = '{0}/MarketSimulator-v1.1-Prod-Output/{1}'.format(user_path, _folder_name)

    zip_path = '{0}/{1}.zip'.format(path, date_str)

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(path)

    data = {'mdgw': _extract_mdgw(path), 'mktsim': _extract_mktsim(path, date),
            'ext-traders': _extract_ext_trader(path)}

    return data
