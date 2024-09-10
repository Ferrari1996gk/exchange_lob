import numpy as np
import pandas as pd
from scipy.stats import kurtosis, skew, ks_2samp
turquoise_datetime_format = "%d-%b-%Y %H:%M:%S.%f"


def get_historical_ret(file, time_format, dt, scale, start, end, freq='1S'):
    lob = pd.read_csv(file, header=0)
    lob['time'] = pd.to_datetime(lob['time'], format=time_format) - dt
    lob.set_index('time', inplace=True)
    lob = lob.loc[start:end]
    new_lob = lob[~lob.index.duplicated(keep="first")]
    del lob
    raw_mid = (new_lob['AskPrc_1'] + new_lob['BidPrc_1']) * 0.5 / scale
    tmp = raw_mid.resample(freq).last().fillna(method='ffill').dropna()
    close = tmp.bfill().values
    h_ret = close[1:] / close[:-1] - 1
    return h_ret, close


def get_synthetic_market_data_sim(data_path):
    df_lob_l1 = pd.read_csv('{0}/lob_l1.csv'.format(data_path)).set_index('time')
    df_lob_l1.index = pd.to_datetime(df_lob_l1.index, format=turquoise_datetime_format)

    l1B = df_lob_l1[df_lob_l1['side'] == 'B']
    l1S = df_lob_l1[df_lob_l1['side'] == 'S']

    mp = .5 * l1B['prc'] + .5 * l1S['prc']

    md = {'mp': mp}
    return md


def hill_estimator(data, tail=0.05):
    """
    Returns the Hill Estimators for some 1D data set.
    """
    data = abs(data)
    Y = np.sort(data)[::-1]
    k = int(tail * len(Y))
    while Y[k] < 1e-20:
        tail -= 0.01
        if tail <= 0.02:
            return 1.
        k = int(tail * len(Y))
    tmp = Y[:k]
    summ = np.sum(np.log(tmp / tmp[-1]))
    hill_est = (1. / k) * summ
    return hill_est


def get_simulated_ret(md, start, end, freq='1S'):
    mp = md['mp']
    mp = mp.dropna().loc[start:end]
    mp_sec = mp.resample(freq).last().fillna(method='ffill').dropna()
    close = mp_sec.bfill().values
    sim_ret = close[1:] / close[:-1] - 1
    return sim_ret


def get_auto_correl(X, lags):
    c = {}
    for lag in lags:
        c[lag] = np.corrcoef(X[:-lag], X[lag:])[0][1]
        
    return pd.Series(c)


def get_auto_correl_matrix(X, lags):
    c = {}
    for lag in lags:
        tmp = np.corrcoef(X[:-lag], X[lag:], rowvar=False)
        c[lag] = np.diagonal(tmp, offset=X.shape[1]).mean()
    
    return pd.Series(c)


# Distance Calculation
# Volatility
def volatility_diff(his_ret, sim_ret):
    scale = np.sqrt(len(his_ret)*252)
    diff = np.std(sim_ret) * scale - np.std(his_ret) * scale
    return abs(diff)


# Auto-correlation diff
# For return first order acf, use small lags
def acf_diff(his_ret, sim_ret, lags):
    acf1 = get_auto_correl(his_ret, lags)
    acf2 = get_auto_correl(sim_ret, lags)
    diff = np.abs(acf1 - acf2)
    return np.mean(diff)


# TODO: weights according to bootstrapped variance
def prepare_distance(his_ret, sim_ret, lags, lags2):
    vol_diff = volatility_diff(his_ret, sim_ret)
    # cdf_ks = ks_2samp(his_ret, sim_ret).statistic
    fat_tail = abs(hill_estimator(his_ret) - hill_estimator(sim_ret))
    ret1_acf_diff = acf_diff(his_ret, sim_ret, lags=lags)
    ret2_acf_diff = acf_diff(his_ret**2, sim_ret**2, lags=lags2)
    
    return np.array([vol_diff, fat_tail, ret1_acf_diff, ret2_acf_diff])
