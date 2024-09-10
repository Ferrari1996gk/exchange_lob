__author__ = "Krishnen Vytelingum"
__copyright__ = "Copyright 2021, Simudyne"
__version__ = "0.1"
__maintainer__ = "Krishnen Vytelingum"
__email__ = "krishnen@simudyne.com"
__status__ = "Prototype"

import zipfile

import pandas as pd

from analytics.prod.extract import get_data
from analytics.trqx.turquoise_exchange import get_market_data


def main():
    #user_path = sys.argv[1]
    #date_str = sys.argv[2]
    user_path = '/Users/krishnen/Simudyne Limited/Data - Documents/LSE Market Data'
    results_path = '{0}/{1}'.format(user_path, 'QM-MarketSimulator/Results')
    workflow = 'VAL'
    date_str = '2020-08-11'
    date = pd.to_datetime(date_str)
    mic = 'XLON'
    symbol = 'SIMUl'
    lit_path = '{0}/{1}/MC/{2}/run_000/{3}'.format(results_path, workflow, date_str, symbol)
    lob_path = '{0}/{1}/MC/{2}/run_000'.format(results_path, workflow, date_str)
    tick_size = .02

    '/Users/krishnen/Simudyne Limited/Data - Documents/LSE Market Data/QM-MarketSimulator/Results/VAL_6M-autoregressive/MC/2020-07-01/run_001/SIMUl'
    md = get_market_data(lit_path, lob_path, symbol, mic, date, tick_size)
    print(md.keys())


if __name__ == '__main__':
    main()
