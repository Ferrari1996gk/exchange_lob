from datetime import datetime
from typing import Union

import logging

import numpy as np
import pandas as pd

BUY = 0
SELL = 1
turquoise_date_file_format = "%Y%m%d"
turquoise_datetime_format = "%d-%b-%y %H.%M.%S.%f"

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


class parser:
    def __init__(self, symbol: str, date: datetime, data_path: str):
        self.symbol = symbol
        self.date = date
        self.data_path = data_path

        lit_path = '{0}/{1}'.format(data_path, 'L2/data-x.londonstockexchange.com/data-x/TRQX')
        trqx_table = pd.read_csv('{0}/symbols.csv'.format(lit_path)).set_index('SYMBOL')
        self.mic = trqx_table.loc[symbol]['MIC']

        self.litOrders = load_lit_orders(lit_path, date, symbol, self.mic)
        self.litTrades = load_lit_trades(lit_path, date, symbol, self.mic)
        self.t0 = pd.to_datetime('{0} 08:00:00'.format(date.strftime('%Y-%m-%d')))
        self.lob, self.lobvv = self.get_durations()
        self.validate()

    def validate(self):
        _l1 = self.get_l1(self.litTrades.index.values, post_clearing=True)
        _spread = _l1['best_ask_prc'] - _l1['best_bid_prc']
        if np.any(_spread <= 1e-6):
            logging.error('Invalid market state (negative or zero spread) at \'{0}\' ...'
                          .format(', '.join([i.strftime('%Y-%m-%d') for i in _spread[_spread <= 1e-8].index[:3]])))

    def get_durations(self):
        np.random.seed(1234)

        self.fix_lit_data()

        lit_orders = self.litOrders.copy().reset_index()
        lit_orders['EXECTYPE_'] = lit_orders['EXECTYPE']
        lit_orders.loc[:, 'PUBLIC_ORDER_ID_'] = lit_orders['PUBLIC_ORDER_ID'].values
        lit_orders.loc[lit_orders['EXECTYPE'] != 'Insert', 'PUBLIC_ORDER_ID_'] = np.nan

        x0 = lit_orders[lit_orders['EXECTYPE'] == 'Amend'].copy()
        x0['EXECTYPE'] = 'Cancel'
        x1 = lit_orders[lit_orders['EXECTYPE'] == 'Amend'].copy()
        x1['EXECTYPE'] = 'Insert'
        x1['PUBLIC_ORDER_ID_'] = ['{0}_{1}'.format(i, np.random.randint(100000000)) for i in x1['PUBLIC_ORDER_ID']]

        x2 = lit_orders[lit_orders['EXECTYPE'] == 'Fill'].copy()
        x2['EXECTYPE'] = 'Cancel'
        x3 = lit_orders[lit_orders['EXECTYPE'] == 'Fill'].copy()
        x3['EXECTYPE'] = 'Insert'
        x3['PUBLIC_ORDER_ID_'] = ['{0}_{1}'.format(i, np.random.randint(100000000)) for i in x3['PUBLIC_ORDER_ID']]

        x = pd.concat([lit_orders[(lit_orders['EXECTYPE'] == 'Insert') |
                                  (lit_orders['EXECTYPE'] == 'Cancel')], x0, x1, x2, x3]).reset_index()
        x = x.sort_values(['TRANSACTTIME', 'index'], ascending=[True, True]).set_index('TRANSACTTIME').iloc[:, 1:] \
            .reset_index()

        x = x.drop(x[(x['EXECTYPE'] == 'Insert') & (x['VISIBLEQTY'] == 0)].index)
        x = x.reset_index().reset_index().sort_values(['PUBLIC_ORDER_ID', 'TRANSACTTIME', 'index']).iloc[:, 2:]

        err_oids = x.groupby(['PUBLIC_ORDER_ID', 'EXECTYPE']).size().unstack().replace(np.nan, 0) \
            .diff(axis=1)['Insert'].replace(0, np.nan).dropna().index
        x = x.drop(x[x['PUBLIC_ORDER_ID'].isin(err_oids)].index)
        x['PUBLIC_ORDER_ID_'] = x['PUBLIC_ORDER_ID_'].fillna(method='ffill')

        xi = x[x['EXECTYPE'] == 'Insert'].reset_index().set_index('PUBLIC_ORDER_ID_').copy()
        xc = x[x['EXECTYPE'] == 'Cancel'].reset_index().set_index('PUBLIC_ORDER_ID_').copy()

        if np.any(xc.index.duplicated()):
            logging.warning('Multiple insert+cancel orders: {0}.'.format(set(xc[xc.index.duplicated()].index.values)))
            for u in xc[xc.index.duplicated()].index:
                j = x[x['PUBLIC_ORDER_ID'] == u].copy()
                j.loc[j['EXECTYPE'] == 'Cancel', 'PUBLIC_ORDER_ID_'] = np.nan
                j.loc[j['EXECTYPE'] == 'Insert', 'PUBLIC_ORDER_ID_'] = j.loc[
                    j['EXECTYPE'] == 'Insert', 'PUBLIC_ORDER_ID_'].apply(
                    lambda i: '{0}_{1}'.format(i, np.random.randint(100000000)))
                j['PUBLIC_ORDER_ID_'] = j['PUBLIC_ORDER_ID_'].fillna(method='ffill')
                x.loc[x['PUBLIC_ORDER_ID'] == u] = j.values

            xi = x[x['EXECTYPE'] == 'Insert'].reset_index().set_index('PUBLIC_ORDER_ID_').copy()
            xc = x[x['EXECTYPE'] == 'Cancel'].reset_index().set_index('PUBLIC_ORDER_ID_').copy()

        xi['START'] = xi['TRANSACTTIME']
        xc['END'] = xc['TRANSACTTIME']

        if not xc[xc.index.duplicated()].empty:
            logging.error('Duplicate:\n', xc[xc.index.duplicated()])

        lob = pd.concat([xi[['PUBLIC_ORDER_ID', 'SIDE', 'PRICE', 'VISIBLEQTY', 'START']],
                         xc.reindex(xi.index)[['PUBLIC_ORDER_ID', 'SIDE', 'END']]], axis=1)
        assert (np.all(lob.iloc[:, 0] == lob.iloc[:, 5]))
        assert (np.all(lob.iloc[:, 1] == lob.iloc[:, 6]))
        lob = lob.iloc[:, [0, 1, 2, 3, 4, 7]].sort_values('START')

        # Pre-process file for numpy handling.
        lobv = lob.copy()
        lobv['SIDE'] = lobv['SIDE'].replace({'Buy': 0, 'Sell': 1})
        _lobvv = lobv.values
        df = np.zeros(len(_lobvv), dtype={'names': ('SIDE', 'PRICE', 'VISIBLEQTY', 'START', 'END', 'PUBLIC_ORDER_ID'),
                                          'formats': ('u1', 'f8', 'u8', 'f8', 'f8', 'U12')})
        df['SIDE'] = _lobvv[:, 1]
        df['PRICE'] = _lobvv[:, 2]
        df['VISIBLEQTY'] = _lobvv[:, 3]
        df['START'] = [self._get_duration(i) for i in _lobvv[:, 4]]
        df['END'] = [self._get_duration(i) for i in _lobvv[:, 5]]
        df['PUBLIC_ORDER_ID'] = _lobvv[:, 0]
        return lob, df

    def fix_lit_data(self):
        o_ids = self.litOrders[self.litOrders['EXECTYPE'] == 'Insert']['PUBLIC_ORDER_ID'].unique()
        fo_ids = self.litOrders[self.litOrders['EXECTYPE'] == 'Fill']['PUBLIC_ORDER_ID'].unique()
        trd_ids = self.litTrades['PUBLICORDERID'].unique()
        missing_fill_ids = set(trd_ids).intersection(set(o_ids)) - set(fo_ids)
        if len(missing_fill_ids) == 0:
            return

        logging.warning('Missing fill ids: {0}.'.format(missing_fill_ids))

        _u = self.litOrders.copy()
        _u['INDEX'] = np.arange(_u.shape[0])
        _u['MISSING'] = 0
        u = []
        for _id in list(missing_fill_ids):
            _t = self.get_litTrades_by_id(_id).index
            trd = self.get_litTrades_by_id(_id).iloc[0]
            r = self.get_litOrders_by_id(_id).loc[_t].copy()
            q = r['VISIBLEQTY'][-1] - trd['EXECUTEDSIZE']
            u = u + [pd.DataFrame([[trd['PUBLICORDERID'], 'Fill', 'Sell', trd['EXECUTEDPRICE'], q,
                                    trd['EXECUTEDSIZE'], 1000000 + self._get_duration(_t[0]), 1]], index=_t,
                                  columns=['PUBLIC_ORDER_ID', 'EXECTYPE', 'SIDE', 'PRICE', 'VISIBLEQTY',
                                           'EXECUTED_QUANTITY', 'INDEX', 'MISSING'])]

        uf = pd.concat([_u] + u).reset_index().sort_values(['TRANSACTTIME', 'INDEX']).set_index('TRANSACTTIME')
        self.litOrders = uf.drop(['INDEX'], axis=1)

    def get_litTrades_by_id(self, _id: Union[list, str]):
        if type(_id) == str:
            return self.litTrades[self.litTrades['PUBLICORDERID'] == _id]

        return self.litTrades[self.litTrades['PUBLICORDERID'].isin(_id)]

    def get_litOrders_by_id(self, _id: Union[list, str]):
        if type(_id) == str:
            return self.litOrders[self.litOrders['PUBLIC_ORDER_ID'] == _id]

        return self.litOrders[self.litOrders['PUBLIC_ORDER_ID'].isin(_id)]

    def get_litTrades_by_time(self, t: Union[np.datetime64, list, np.ndarray]):
        return self.litTrades.loc[t]

    def get_litOrders_by_time(self, t: Union[np.datetime64, list, np.ndarray]):
        return self.litOrders.loc[t]

    def get_l1(self, t: Union[np.datetime64, list, np.ndarray], post_clearing=True):
        if type(t) == np.datetime64:
            _l1vb = self._get_l1v_by_side([t], BUY, post_clearing)
            _l1va = self._get_l1v_by_side([t], SELL, post_clearing)
            df = pd.Series({'buy_depth': _l1vb['depth'], 'total_bid_vol': _l1vb['total_vol'],
                            'best_bid_vol': _l1vb['best_vol'], 'best_bid_prc': _l1vb['best_prc'],
                            'best_ask_prc': _l1va['best_prc'], 'best_ask_vol': _l1va['best_vol'],
                            'total_ask_vol': _l1va['total_vol'], 'sell_depth': _l1va['depth']})
            df.name = 'TRANSACTTIME'
            return df

        if type(t) == list or type(t) == np.ndarray:
            _l1vb = self._get_l1v_by_side(t, BUY, post_clearing)
            _l1va = self._get_l1v_by_side(t, SELL, post_clearing)
            df = pd.concat([pd.DataFrame(_l1vb), pd.DataFrame(_l1va)], axis=1)
            df.index = t
            df.columns = ['best_bid_prc', 'best_bid_vol', 'total_bid_vol', 'buy_depth', 'best_ask_prc', 'best_ask_vol',
                          'total_ask_vol', 'sell_depth']
            df.index.name = 'TRANSACTTIME'
            return df.astype({'buy_depth': int, 'total_bid_vol': int, 'best_bid_vol': int, 'best_bid_prc': float,
                              'best_ask_prc': float, 'best_ask_vol': int, 'total_ask_vol': int, 'sell_depth': int})

        logging.error('Unknown type {0}'.format(type(t)))
        raise TypeError

    def get_l2(self, t, post_clearing=True, axis=1):
        cols = ['PRICE', 'VISIBLEQTY', 'NUM']
        df = pd.concat({_decode_side(side): pd.DataFrame(self._get_l2v_by_side(t, side, post_clearing)[cols],
                                                         columns=cols) for side in [BUY, SELL]}, axis=axis)
        df.index.name = 'LEVEL'
        df.name = t
        return df

    def get_l3(self, t, axis=1, post_clearing=True):
        cols = ['PRICE', 'VISIBLEQTY', 'PUBLIC_ORDER_ID']
        df = pd.concat(
            {_decode_side(side): pd.DataFrame(self._get_l3v_by_side(t, side, post_clearing)[cols], columns=cols)
             for side in [BUY, SELL]}, axis=axis)
        df.index.name = 'LEVEL'
        df.name = t
        return df

    def _get_l1v_by_side(self, t, side, post_clearing):
        j = [self._filter_lobvv(_t, side, post_clearing) for _t in t]
        lj = len(j)
        df = np.zeros(max([1, lj]), dtype={'names': ('best_prc', 'best_vol', 'total_vol', 'depth'),
                                           'formats': ('f8', 'u8', 'u8', 'u8')})
        if lj > 0:
            bb = [np.nan if np.all(np.isnan(_J['PRICE'])) else
                  (1 - side) * np.nanmax(_J['PRICE']) + side * np.nanmin(_J['PRICE']) for _J in j]
            df['best_prc'] = bb
            df['best_vol'] = [j[i][j[i]['PRICE'] == bb[i]]['VISIBLEQTY'].sum() for i in np.arange(lj)]
            df['total_vol'] = [np.sum(_J['VISIBLEQTY']) for _J in j]
            df['depth'] = [len(_J) if np.sum(_J['VISIBLEQTY']) > 0 else 0 for _J in j]
        else:
            df['best_prc'] = None
            df['best_vol'] = 0
            df['total_vol'] = 0
            df['depth'] = 0

        return df

    def _get_l2v_by_side(self, t, side, post_clearing):
        _J = self._get_l3v_by_side(t, side, post_clearing)
        a, b = np.unique(_J['PRICE'], return_counts=True)
        ii = np.argsort(a)[::-1] if side == BUY else np.argsort(a)
        df = np.zeros(len(a), dtype={'names': ('PRICE', 'VISIBLEQTY', 'NUM'), 'formats': ('f8', 'u8', 'u8')})
        df['PRICE'] = a[ii]
        df['VISIBLEQTY'] = [np.sum(_J[_J['PRICE'] == i]['VISIBLEQTY']) for i in a[ii]]
        df['NUM'] = b[ii]
        return df

    def _get_l3v_by_side(self, t, side, post_clearing):
        _J = self._filter_lobvv(t, side, post_clearing)
        return _J[_J['PRICE'].argsort()[::-1]] if side == BUY else _J[_J['PRICE'].argsort()]

    def _filter_lobvv(self, t, side, post_clearing):
        t_ = self._get_duration(t)
        t_end_ = t_ + float(post_clearing) * 1e-6
        df = self.lobvv[(self.lobvv['START'] <= t_) & (self.lobvv['END'] >= t_end_) & (self.lobvv['SIDE'] == side)]

        _ids = np.unique(df['PUBLIC_ORDER_ID'])
        _df = np.zeros(max([1, len(_ids)]), dtype={'names': ('SIDE', 'PRICE', 'VISIBLEQTY', 'PUBLIC_ORDER_ID'),
                                                   'formats': ('u1', 'f8', 'u8', 'U12')})
        if len(_ids) > 0:
            _df['SIDE'] = side
            _df['VISIBLEQTY'] = np.array([np.sum(df[df['PUBLIC_ORDER_ID'] == i]['VISIBLEQTY']) for i in _ids])
            _df['PRICE'] = np.array([df[df['PUBLIC_ORDER_ID'] == i]['PRICE'][0] for i in _ids])
            _df['PUBLIC_ORDER_ID'] = _ids
        else:
            _df['SIDE'] = side
            _df['PRICE'] = None
            _df['VISIBLEQTY'] = 0
            _df['PUBLIC_ORDER_ID'] = None

        return _df

    def _get_duration(self, t):
        return (t - self.t0).total_seconds()


def load_lit_orders(lit_path, date, symbol, mic):
    return _load_lit('LitOrders', lit_path, date, symbol, mic, ['TRANSACTTIME', 'PUBLIC_ORDER_ID', 'EXECTYPE', 'SIDE',
                                                                'PRICE', 'VISIBLEQTY', 'EXECUTED_QUANTITY'])


def load_lit_trades(lit_path, date, symbol, mic):
    return _load_lit('LitTrades', lit_path, date, symbol, mic, ['PUBLICORDERID', 'TRANSACTTIME', 'SIDE',
                                                                'EXECUTEDPRICE', 'EXECUTEDSIZE', 'TRADEREPORTID'])


def _load_lit(dataset, lit_path, date, symbol, mic, columns):
    _datestr = date.strftime(turquoise_date_file_format)
    _Lit_path = '{0}/{4}/{1}/{1}_{2}_{3}_{4}.csv'.format(lit_path, symbol, mic, _datestr, dataset)
    __lit = pd.read_csv(_Lit_path, parse_dates=['TRANSACTTIME'],
                        date_parser=lambda x: datetime.strptime(x, turquoise_datetime_format))
    return __lit[columns].reset_index().sort_values(['TRANSACTTIME', 'index']).set_index('TRANSACTTIME').iloc[:, 1:]


def _get_side(side):
    return BUY if side == 'Buy' else SELL


def _decode_side(side):
    return 'Buy' if side == BUY else 'Sell'


def main():
    symbol = 'BARCl'
    date = pd.to_datetime('2022-03-16')
    data_path = '/Users/perukrishnenvytelingum/Downloads/Mktsim/TRQX'
    trqx_parser = parser(symbol, date, data_path)

    #np.random.seed(1234)
    #ts0 = np.unique(trqx_parser.litOrders.index.values)
    #print(trqx_parser.get_l1(ts0[:3], post_clearing=True))

    print(trqx_parser.litOrders.loc[['2022-03-16 08:00:16.772109']])
    print(trqx_parser.get_l3(pd.to_datetime('2022-03-16 08:00:16.772108'), post_clearing=True))


if __name__ == '__main__':
    main()
