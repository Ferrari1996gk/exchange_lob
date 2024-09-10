import numpy as np
import pandas as pd
import scipy.stats as sc
from functools import reduce


# GSL-div analysis
def get_symbolised_ts(ts, b, L, min_per=1, max_per=99, state_space=None):
    # if no state space is defined we generate our own based on
    # either the standard percentiles or those given by the user
    cuts = []
    if not state_space:
        for x in ts:
            min_p = np.percentile(x, min_per)
            max_p = np.percentile(x, max_per)
            cuts.append(np.linspace(min_p, max_p, b+1))
    else:
        cuts = np.linspace(state_space[0], state_space[1], b+1)
    # now we map the time series to the bins in the symbol space
    symbolised_ts = np.array([np.clip(np.digitize(t, cut, right=True), 1, b) for t, cut in zip(ts,cuts)])
    # to be able to deal with "words" or combination of symbols it is easier
    # to deal with them as strings in pandas dfs
    # TODO: Maybe better way of doing this
    sym_str = pd.DataFrame(symbolised_ts).astype(str)
    # collect all symbol dataframes based on block size given
    all_dfs = []
    all_dfs.append(sym_str)
    tmp = sym_str.values
    for l in range(1, L):
        tmp = tmp[:, :-1] + sym_str.values[:, l:]
        all_dfs.append(pd.DataFrame(tmp))
    return all_dfs


def get_weights(weight_type, L):
    if weight_type == 'uniform':
        w = np.array([1. / L] * L)
    elif weight_type == 'add-progressive':
        w = np.array([2. / (L * (L + 1))]*L).cumsum()
    return w


def gsl_div(original, model, weights='add-progressive', b=5, L=6, min_per=1, max_per=99, state_space=None):
    all_ts = np.concatenate([original, model])
    # determine the time series length
    T = original.shape[1]
    if T < L:
        raise ValueError('Word length cant be longer than timeseries')
    # symbolise time-series
    sym_ts = get_symbolised_ts(all_ts, b=b, L=L, min_per=min_per, max_per=max_per, state_space=state_space)
    raw_divergence = []
    correction = []
    # run over all word sizes
    for n, ts in enumerate(sym_ts):
        # get frequency distributions for original and replicates
        # could do it by applying a pd.value_counts to every column but this
        # way you get a 10x speed up
        fs = pd.DataFrame((ts.stack().reset_index().groupby([0, "level_0"]).count() / len(ts.T)).unstack().values.astype(float))

        # replace NaN with 0 so that log does not complain later
        fs = fs.replace(np.nan, 0)
        # determine the size of vocabulary for the right base in the log
        base = b**(n+1)
        # calculate the distances between the different time-series
        # give a particluar word size
        M = (fs.iloc[:, 1:].values + np.expand_dims(fs.iloc[:, 0].values, 1)) / 2.
        temp = (2 * sc.entropy(M, base=base) - sc.entropy(fs.values[:, 1:], base=base))
        raw_divergence.append(reduce(np.add, temp) / float((len(fs.columns) - 1)))
        cardinality_of_m = fs.apply(lambda x: reduce(np.logical_or, x), axis=1).sum()
        # if there is only one replicate this has to be handled differently
        if len(fs.columns) == 2:
            cardinality_of_reps = fs.iloc[:, 1].apply(lambda x: x != 0).sum()
        else:
            cardinality_of_reps = fs.iloc[:, 1:].apply(
                lambda x: reduce(np.logical_or, x), axis=1).sum()
        # calculate correction based on formula 9 line 2 in paper
        correction.append(2*((cardinality_of_m - 1) / (4. * T)) - ((cardinality_of_reps - 1) / (2. * T)))

    w = get_weights(weight_type=weights, L=L)
    weighted_res = (w * np.array(raw_divergence)).sum(axis=0)
    weighted_correction = (w * np.array(correction)).sum()

    return weighted_res + weighted_correction


def split_gsl_div(ret_h, ret_s, b=5, L=10, min_per=1, max_per=99, unit=1000):
    length = min(len(ret_h), len(ret_s))
    res_list = []
    for i in range(length//unit):
        h_ts = ret_h[(i*unit):((i+1)*unit)].reshape(1, -1)
        s_ts = ret_s[(i*unit):((i+1)*unit)].reshape(1, -1)
        res_list.append(gsl_div(h_ts, s_ts, 'add-progressive', b=b, L=L, min_per=min_per, max_per=max_per))
    # print(res_list)
    return np.mean(res_list), res_list


def nosplit_gsl_div(ret_h, ret_s, b=5, L=10, min_per=1, max_per=99):
    length = min(len(ret_h), len(ret_s))
    h_ts = ret_h[:length].reshape(1, -1)
    s_ts = ret_s[:length].reshape(1, -1)
    value = gsl_div(h_ts, s_ts, 'add-progressive', b=b, L=L, min_per=min_per, max_per=max_per)
    return value
