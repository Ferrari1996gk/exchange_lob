__author__ = "Krishnen Vytelingum"
__copyright__ = "Copyright 2021, Simudyne"
__version__ = "0.1"
__maintainer__ = "Krishnen Vytelingum"
__email__ = "krishnen@simudyne.com"
__status__ = "Prototype"

import os
import numpy as np
import pandas as pd

from datetime import datetime

"""
Description: Set of utility functions for an agent-based market simulator.
"""


def get_dates(path: str, symbol: str) -> np.array([datetime]):
    """
    Return all the dates of data available for symbol in the folder path.
    :param path: path of folder
    :param symbol: symbol to read
    :return: list of datetime available
    """
    _path = '{0}/LitTrades/{1}'.format(path, symbol)
    if os.path.isdir(_path):
        return np.sort(np.unique(([pd.to_datetime(os.path.join(file).split('_')[2],
                                                  format='%Y%m%d') for file in os.listdir(_path)
                                   if file.endswith('_LitTrades.csv')])))
    else:
        raise IOError('LitTrades directory not found.')


def flatten(t: list):
    """Return a flattened list"""
    return [item for sublist in t for item in sublist]
