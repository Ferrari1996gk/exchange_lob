from pykalman import KalmanFilter
import scipy.interpolate as interpolate
import numpy as np
import pandas as pd


# Get fundamental value from Kalman smoothing and interpolation
def Kalman1D(observations,damping=1):
    # To return the smoothed time series data
    observation_covariance = damping
    initial_value_guess = observations[0]
    transition_matrix = 1
    transition_covariance = 0.1
    initial_value_guess
    kf = KalmanFilter(
            initial_state_mean=initial_value_guess,
            initial_state_covariance=observation_covariance,
            observation_covariance=observation_covariance,
            transition_covariance=transition_covariance,
            transition_matrices=transition_matrix
        )
    pred_state, state_cov = kf.smooth(observations)
    return pred_state


def get_damping_factor(P):
    g = {}
    for d in np.concatenate([np.array([1e-2]), np.arange(1,10,3), np.arange(10,151,10)]):
        ve = Kalman1D(P, damping=d)[:,0]
        g[d] = pd.Series(1e4 * (ve[1:] / ve[:-1] - 1)).std()
    thres = 1e4 * np.std(P[1:] / P[:-1] - 1) / 10. # TODO
    # print(pd.Series(g))
    return (pd.Series(g) < thres).idxmax()


def get_interpolated_v(prc_1s, damping_factor=20, up_freq=10):
    Ve = Kalman1D(prc_1s, damping=damping_factor)[:,0]
    x = np.array(range(0, len(prc_1s) * up_freq, up_freq))
    f = interpolate.interp1d(x, Ve, kind='cubic')
    newx = np.arange(0, (len(prc_1s)-1) * up_freq)
    inter_mid = f(newx)
    return inter_mid
