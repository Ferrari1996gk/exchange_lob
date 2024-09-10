__author__ = "Krishnen Vytelingum"
__copyright__ = "Copyright 2021, Simudyne"
__version__ = "0.1"
__maintainer__ = "Krishnen Vytelingum"
__email__ = "krishnen@simudyne.com"
__status__ = "Prototype"

import sys

import pandas as pd

from analytics.prod.extract import get_data
from analytics.prod.extract import get_data, _extract_mdgw, _extract_mktsim


def main():
    user_path = sys.argv[1]
    date_str = sys.argv[2]
    date = pd.to_datetime(date_str)

    data = get_data(user_path, date, run_id='test-1')

    print(data)
    print('Analytics Complete')


if __name__ == '__main__':
    main()
