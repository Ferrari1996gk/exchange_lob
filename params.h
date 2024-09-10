//
// Created by Perukrishnen Vytelingum on 10/09/2020.
//

#ifndef ZEROINTELLIGENCEMARKET_PARAMS_H
#define ZEROINTELLIGENCEMARKET_PARAMS_H

#include <iostream>
#include <map>

using namespace std;

struct zi_base_params {
    double delta;
    double mean;
    double sd;
    unsigned int limit_vol;
    unsigned int market_vol;
};

struct zi_params {
    double alpha;
    double mu;
    zi_base_params ziBaseParams;
};

struct ft_params {
    double kappa_lo;
    double kappa_mo;
    double kappa_lo_3;
    double kappa_mo_3;
    int ft_freq;
    zi_base_params ziBaseParams;
};


struct mt_params {
    double alpha;
    double beta_lo;
    double beta_mo;
    double gamma;
    zi_base_params ziBaseParams;
};

struct hmt_params {
    double halpha;
    double hbeta_lo;
    double hbeta_mo;
    double hgamma;
    zi_base_params ziBaseParams;
};

struct mm_params {
    double mm_delta;
    double mm_lo;
    double mm_mo;
    unsigned int mm_vol;
    int mm_edge;
    int mm_pos_limit;
    int mm_pos_safe;
    unsigned int mm_mkvol;
    long mm_rest;
};

struct ins_params {
    int ins_mode;
    float ins_pov;
    unsigned long long start_step;
    int total_vol;
    unsigned int ins_vol;
    unsigned int ins_interval;
    unsigned int obs_interval;
};

struct st_params {
    double st_mo;
    unsigned int st_vol;
    int st_interval;
};


struct fundamental_value_params {
    float s0;
    float mu;
    float sigma;
    unsigned long long step_size;
    long seed;
    int dump_freq;
    bool is_ready;
    string source_path;
};

struct mc_params {
    long simulationID;
    long seed;
    string date;
    string results_path;
    zi_params ziParams;
    ft_params ftParams;
    mt_params mtParams;
    hmt_params hmtParams;
    mm_params mmParams;
    ins_params insParams;
    st_params stParams;
    fundamental_value_params fvParams;
};

struct sim_params {
    string symbol;
    double closing_bid_prc;
    double closing_ask_prc;
    float tick_size;
    unsigned long long n_steps;
    unsigned long long step_size;
    int n_threads;
    int verbose;
    unsigned int l2_depth;
    string tick_format;
    string date_format;
    int n_runs;
    long seed;
    unsigned int n_zi_traders;
    unsigned int n_ft_traders;
    unsigned int n_mt_traders;
    unsigned int n_hmt_traders;
    unsigned int n_mm_traders;
    unsigned int n_ins_traders;
    unsigned int n_st_traders;
};


#endif //ZEROINTELLIGENCEMARKET_PARAMS_H
