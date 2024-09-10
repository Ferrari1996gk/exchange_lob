sim_params = {
    "reference_prc": 1160,
    "closing_bid_prc": 1160,
    "closing_ask_prc": 1163,
    "tick_size": 0.25,
    "l2_depth": 10,
    "symbol": "ZIl",
    "tick_format": "LSE",
    "date": "2010-05-06",
    "date_format": "%d-%b-%Y %H:%M:%S",
    "n_runs": 18,
    "n_zi_traders": 30,
    "n_ft_traders": 30,
    "n_mt_traders": 30,
    "n_hmt_traders": 30,
    "n_mm_traders": 20,
    "n_ins_traders": 0,
    "n_st_traders": 0,
    "n_mins": 300,
    "step_size": 100000,
    "n_threads": 6,
    "verbose": 2,
    "seed": 123
}

model_params = {
    "ZI_base_params": {
        "delta": 0.005,
        "mean": 2.0,
        "sd": 0.3,
        "limit_vol": 100,
        "market_vol": 100
    },
    "ZI_params": {
        "alpha": 0.2,
        "zi_mo_ratio": 0.2
    },
    "FT_params": {
        "kappa_lo": 0.003,
        "kappa_lo_3": 0.3,
        "kappa_mo_ratio": 1.0,
        "ft_freq": 1
    },
    "MT_params": {
        "beta_lo": 0.3,
        "beta_mo_ratio": 0.2,
        "gamma": 10,
        "alpha": 0.001
    },
    "HMT_params": {
        "hbeta_lo": 0.1,
        "hbeta_mo_ratio": 0.2,
        "hgamma": 10,
        "halpha": 0.9
    },
    "MM_params": {
        "mm_delta": 0.05,
        "mm_lo": 0.4,
        "mm_mo": 0.0,
        "mm_vol": 100,
        "mm_edge": 4,
        "mm_pos_limit": 5000,
        "mm_pos_safe": 101,
        "mm_mkvol": 100,
        "mm_rest": 12000
    },
    "ST_params": {
        "st_mo": 0.005,
        "st_vol": 50,
        "st_interval": 4
    },
    "INS_params": {
        "ins_mode": 1,
        "ins_pov": 0.09,
        "start_step": 234000,
        "total_vol": 120000,
        "ins_vol": 100,
        "ins_interval": 120,
        "obs_interval": 600
    }
}
