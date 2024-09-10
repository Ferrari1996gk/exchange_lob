from CMECalibration.distance_calc import hill_estimator
import numpy as np
from statsmodels.tsa.stattools import acf


def block_bootstrap_ind(length, block_size):
    n_block = length // block_size + 1
    samples = np.random.choice(n_block, size=n_block, replace=True)
    new_ind = np.concatenate([np.arange(i*block_size, (i+1)*block_size) for i in samples])
    new_ind = new_ind[new_ind < length]
    return new_ind


def get_moments(ret_arr, ll, LAGS, LAGS2):
    his_hill = hill_estimator(ret_arr)
    his_vol = np.std(ret_arr) * np.sqrt(len(ret_arr)*252)
    values = acf(ret_arr, fft=True, nlags=max(LAGS))
    his_acf = values[LAGS].reshape((len(LAGS)//ll, ll)).mean(axis=1)
    values = acf(ret_arr**2, fft=True, nlags=max(LAGS2))
    his_acf2 = values[LAGS2].reshape((len(LAGS2)//ll, ll)).mean(axis=1)
    return his_hill, his_vol, his_acf, his_acf2


def get_confidence_interval(his_ret, ll, LAGS, LAGS2, block_size=1200, num=1000, level=0.99):
    np.random.seed(1)
    half_len = 1.96 if level == 0.95 else 2.58
    his_hill, his_vol, his_acf, his_acf2 = get_moments(his_ret, ll, LAGS, LAGS2)
    boot_hill_list, boot_vol_list = [], []
    boot_acf_list, boot_acf2_list = [], []
    for _ in range(num):
        ret_ind = block_bootstrap_ind(len(his_ret), block_size)
        tmp_his_ret = his_ret[ret_ind]
        boot_hill_list.append(hill_estimator(tmp_his_ret))
        boot_vol_list.append(np.std(tmp_his_ret) * np.sqrt(len(tmp_his_ret)*252))
        values = acf(tmp_his_ret, fft=True, nlags=max(LAGS))
        sim_acf = values[LAGS].reshape((len(LAGS)//ll, ll)).mean(axis=1)
        values = acf(tmp_his_ret**2, fft=True, nlags=max(LAGS2))
        sim_acf2 = values[LAGS2].reshape((len(LAGS2)//ll, ll)).mean(axis=1)
        boot_acf_list.append(sim_acf)
        boot_acf2_list.append(sim_acf2)
    hill_sd = np.std(boot_hill_list)
    vol_sd = np.std(boot_vol_list)
    hill_interval = [his_hill - half_len * hill_sd, his_hill + half_len * hill_sd]
    vol_interval = [his_vol - half_len * vol_sd, his_vol + half_len * vol_sd]
    acf_interval = np.array([his_acf - half_len * np.array(boot_acf_list).std(axis=0), his_acf + half_len * np.array(boot_acf_list).std(axis=0)]).T
    acf2_interval = np.array([his_acf2 - half_len * np.array(boot_acf2_list).std(axis=0), his_acf2 + half_len * np.array(boot_acf2_list).std(axis=0)]).T
    return hill_interval, vol_interval, acf_interval, acf2_interval


def get_moment_coverage(s_ret, his_intervals, ll, LAGS, LAGS2):
    n1 = len(LAGS) // ll
    n2 = len(LAGS2) // ll
    sim_hill, sim_vol, sim_acf, sim_acf2 = get_moments(s_ret, ll, LAGS, LAGS2)
    # print(sim_acf, sim_acf2, round(sim_hill, 4), round(sim_vol, 4))
    sim_cover = [0 for _ in range(n1 + n2 + 2)]
    # The order matters!
    if his_intervals['hill'][0] < sim_hill < his_intervals['hill'][1]:
        sim_cover[0] = 1

    if his_intervals['vol'][0] < sim_vol < his_intervals['vol'][1]:
        sim_cover[1] = 1

    for i in range(n1):
        if his_intervals['acf'][i, 0] < sim_acf[i] < his_intervals['acf'][i, 1]:
            sim_cover[i + 2] = 1
    for i in range(n2):
        if his_intervals['acf2'][i, 0] < sim_acf2[i] < his_intervals['acf2'][i, 1]:
            sim_cover[i + n1 + 2] = 1
    if np.sum(sim_cover) == len(sim_cover):
        sim_cover.append(1)
    else:
        sim_cover.append(0)
    return sim_cover
