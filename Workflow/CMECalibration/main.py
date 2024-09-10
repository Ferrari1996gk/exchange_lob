import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import cm
import json
import pyDOE
from smt.sampling_methods import LHS
from datetime import datetime
import scipy.stats as sc
from itertools import chain

from distance_calc import *
from xgboost_surrogate import *
from kalman_fv import *
from calibration_utils import *


def update_sim_params(sim_params, params):
    sim_params['T'] = params['T']
    sim_params['p0'] = params['p0']
    sim_params['gamma'] = params['gamma']


def get_params_range(model_flag):
    n_dims = 6
    params_range = np.array([kappa_list, beta_lf_list, alpha_lf_list, beta_rt_list, alpha_rt_list, sigma_N_list])
    return n_dims, params_range

# function to optimize; maps ABM model parameters to model-real distances
def func_opt(model_params, Ve, sim_params, lags, lags2, run_func, his_ret=None, close=None, weights=None):
    assert (his_ret is None) ^ (close is None), "Should supply exactly one data"
    res = run_func(sim_params, model_params, v=Ve)
    P = res['P']
    if his_ret is not None:
        sim_ret_arr = (P / P.shift(1) - 1).dropna().values
        dist_array = prepare_distance(his_ret, sim_ret_arr, lags, lags2)
        dist = compute_distance(dist_array, weights=weights)
    else:
        tmp = np.square(P.values - close.reshape(-1, 1)) #.sum(axis=0)
        dist = tmp.mean() + tmp.std()
    return max(-1. * dist, np.float64(-1))


def xgboost_modelling(n_dims, total_num, params_range, initial_eval, Ve, sim_params, lags, lags2, run_func, his_ret=None, close=None, weights=None, batch_num=10, n_iters=20, model_flag=0):
    total_samples = get_sobol_samples(n_dims, total_num, params_range)# Total sample space
    np.random.shuffle(total_samples)
    x_evaluated, x_remaining = split_samples(total_samples, number=initial_eval)
    y_evaluated = np.array([])
    for i, xx in enumerate(x_evaluated):
        if i%200==0:
            print(i)
        model_params = get_params(xx, model_flag)
        y_evaluated = np.append(y_evaluated, func_opt(model_params, Ve, sim_params, lags, lags2, run_func, his_ret=his_ret, close=close, weights=weights))
    print('Current best/worst distance: %.4f, %.4f'%(max(y_evaluated), min(y_evaluated)))
    # Fit surrogate using the initial chosen random points
    surrogate_model_XGBoost = fit_surrogate_model(x_evaluated, y_evaluated)
    # Iteratively choose more points and fit the model
    for k in range(n_iters):
        print(k)
        y_hat = surrogate_model_XGBoost.predict(x_remaining)
        print(sorted(y_hat)[-10:-5])
        # ind_eval = y_hat.argsort()[-batch_num:]
        ind_eval = np.concatenate([y_hat.argsort()[-batch_num:], np.random.choice(len(y_hat), size=batch_num//2, replace=False)])
        x_to_eval, x_remaining = split_samples(x_remaining, ind=ind_eval)

        y_to_eval = np.array([])
        for i, xx in enumerate(x_to_eval):
            model_params = get_params(xx, model_flag)
            y_to_eval = np.append(y_to_eval, func_opt(model_params, Ve, sim_params, lags, lags2, run_func, his_ret=his_ret, close=close, weights=weights))

        x_evaluated = np.concatenate([x_evaluated, x_to_eval])
        y_evaluated = np.concatenate([y_evaluated, y_to_eval])
        print('Current best/worst distance: %.4f, %.4f'%(max(y_evaluated), min(y_evaluated)))
        surrogate_model_XGBoost = fit_surrogate_model(x_evaluated, y_evaluated)

    total_y_hat = surrogate_model_XGBoost.predict(total_samples)
    print(total_y_hat.shape, total_y_hat.max())
    global_ind = total_y_hat.argsort()[-5:] # Find the index for the best 5 points
    opt = total_samples[global_ind[-1]] # Find the best point
    print('opt: ', opt)
    pred_y_opt = surrogate_model_XGBoost.predict(opt.reshape(1,-1))
    model_params = get_params(opt, model_flag)
    simulated_y_opt = func_opt(model_params, Ve, sim_params, lags, lags2, run_func, his_ret=his_ret, close=close, weights=weights)
    print('predicted optimal: %.6f; Actual simulated value in the predicted optimal point: %.6f'%(pred_y_opt, simulated_y_opt))
    return opt, surrogate_model_XGBoost, pred_y_opt, simulated_y_opt

def brute_force(n_dims, total_num, params_range, initial_eval, Ve, sim_params, lags, lags2, run_func, his_ret=None, close=None, weights=None, model_flag=0):
    total_samples = get_sobol_samples(n_dims, total_num, params_range)# Total sample space
    y_evaluated = np.array([])
    for i, xx in enumerate(total_samples):
        if i%1000==0:
            print(i)
        model_params = get_params(xx, model_flag)
        y_evaluated = np.append(y_evaluated, func_opt(model_params, Ve, sim_params, lags, lags2, run_func, his_ret=his_ret, close=close, weights=weights))

    global_ind = y_evaluated.argsort()[-5:] # Find the index for the best 5 points
    opt = total_samples[global_ind[-1]] # Find the best point
    print('opt: ', opt)
    print('Optimal point value in total space: %.6f %.6f'%(y_evaluated.max(), y_evaluated[global_ind[-1]]))
    return opt, y_evaluated, total_samples


if __name__ == "__main__":
    print("Hello")
    cme_format = '%Y%m%d%H%M%S%f'
    dt = datetime.timedelta(hours=4)
    ticker = "ESM0"
    datestr = "20100505"
    directory = 'D:/projects/data/XCME/2010/{0}/{1}/MD/XCME/'.format(datestr[-4:-2], datestr[-2:])
    start = pd.to_datetime(datestr + '080000', format=cme_format)
    end = pd.to_datetime(datestr + '170000', format=cme_format)
    from params import sim_parms, model_params
    zi_mean_list = [0.1, 4]
    sigma_N_list = [0.01, 1.0]
    kappa_list = [0.01, 0.5]
    kappa_3_list = [0.01, 0.5]
    beta_lf_list = [0.01, 1]
    beta_hf_list = [0.01, 1]
    mm_lo_list = [0.1, 1]

    LAGS = list(chain(*[[x, x+1, x+2] for x in (1, 30, 60, 300, 600)]))
    LAGS2 = list(chain(*[[x, x+1, x+2] for x in (1, 30, 60, 300, 600)]))

    total_num = 16384
    initial_eval = 2000
    batch_num=200
    n_iters=3
    weights = np.array([1,1,1,1])
    gamma = 10

    s_time = ' 08:00:00'
    e_time = ' 17:00:00'
    directory = "./data/{0}/{1}/".format(exchange, ticker)
    file_list = sorted(os.listdir(directory))
    print(ticker)
    params_record = {}
    with open("./xgb_eval/%s.json"%ticker, 'r', encoding='utf-8') as f:
        xgb_eval_record = json.load(f)
        f.close()

    for file_name in file_list:
        datestr = file_name[:8]
        print(datestr)
        file = directory+datestr+".csv"

        his_ret, close, data = get_historical_ret(file, start, end)
        damping_factor = get_damping_factor(close)
        print('damping factor: %.2f'%damping_factor)
        Ve = Kalman1D(close, damping=damping_factor)[:,0]

        params = {'T': len(Ve), 'p0': close[0], 'gamma': gamma}
        update_sim_params(sim_params, params)
        print(sim_params)
        np.random.seed(1234)

        for model_flag in model_list:
            saved_params_file = './model_params/'+str(model_flag)+'/'+ticker+'.json'
            with open(saved_params_file, 'r', encoding='utf-8') as f:
                params_record[model_flag] = json.load(f)
                f.close()

            if datestr in params_record[model_flag].keys():
                print("Already calibrated! Skip %s for model %d"%(datestr, model_flag))
                break

            run_func = get_func(model_flag)
            n_dims, params_range = get_params_range(model_flag)

            opt, surrogate, pred_y_opt, simulated_y_opt = xgboost_modelling(n_dims, total_num, params_range, initial_eval, Ve,
                                                                            sim_params, LAGS, LAGS2, run_func, his_ret=his_ret, close=None, weights=weights,
                                                                            batch_num=batch_num, n_iters=n_iters, model_flag=model_flag)

            opt_dict = get_opt_dict(opt, model_flag)
            print(opt_dict)

            params_record[model_flag][datestr] = opt_dict
            with open(saved_params_file, 'w', encoding='utf-8') as f:
                json.dump(params_record[model_flag], f, ensure_ascii=False, indent=2)
                f.close()

            xgb_eval_record[datestr] = {'xgb': np.asscalar(pred_y_opt), 'sim': np.asscalar(simulated_y_opt)}
            with open("./xgb_eval/%s.json"%ticker, 'w', encoding='utf-8') as f:
                json.dump(xgb_eval_record, f, ensure_ascii=False, indent=2)
                f.close()
