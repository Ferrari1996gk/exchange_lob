import numpy as np
import pandas as pd
import subprocess
import json
import datetime


# Auxiliary functions
def update_model_params(model_params, params):
    model_params['ZI_base_params']['mean'] = params['zi_mean']
    model_params['ZI_params']['alpha'] = params['n_sigma']
    model_params['FT_params']['kappa_lo'] = params['f_kappa']
    model_params['FT_params']['kappa_lo_3'] = params['f_kappa_3']
    model_params['MT_params']['beta_lo'] = params['lf_beta']
    model_params['HMT_params']['hbeta_lo'] = params['hf_beta']
    model_params['MM_params']['mm_lo'] = params['mm_lo']


def update_sim_params(sim_params, prc, tick_size, datestr, n_runs=3):
    sim_params['closing_bid_prc'] = (prc // tick_size - 1) * tick_size
    sim_params['closing_ask_prc'] = (prc // tick_size + 1) * tick_size
    sim_params['tick_size'] = tick_size
    sim_params['date'] = datetime.datetime.strftime(pd.to_datetime(datestr), "%Y-%m-%d")
    sim_params['n_runs'] = n_runs


def save_target_params(target_params, save_path):
    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(target_params, f, ensure_ascii=False, indent=2)
        f.close()


def run_simulation():
    cmd = "D:/projects/simulator/cmake-build-release/SimulatedMarket.exe CME True"
    process = subprocess.Popen(cmd, stdout=None, cwd=r'D:/projects/simulator/cmake-build-release/')
    output = process.wait()
    return output


# auxiliary function for xgboost surrogate modelling
def split_samples(remaining, number=None, ind=None):
    if ind is None:
        assert number is not None, "Must provide number if no ind provided!"
        ind = np.random.choice(len(remaining), size=number, replace=False)
    rem_ind = list(set(range(len(remaining))) - set(ind))
    return remaining[ind], remaining[rem_ind]
