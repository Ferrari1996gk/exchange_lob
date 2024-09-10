#include <iostream>
#include <thread>

#include "simulator.h"
#include "params.h"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/filesystem.hpp>

using namespace std;

void run_task(int threadID, sim_params simParams, std::vector<mc_params> mcParams) {

    int n_runs = mcParams.size();
    for(int i = 0; i < n_runs; ++i) {

        ifstream f(mcParams[i].results_path + "/status.txt");
        if(f.good()) {
            std::string line;
            if(getline(f, line) && line == "OK") {
                continue;
            }
        }

        auto start = std::chrono::high_resolution_clock::now();

        long seed = mcParams[i].seed;

        simulator simulator(&simParams, mcParams[i]);

        simulator.run();

        fstream fs;
        fs.open(mcParams[i].results_path + "/status.txt", std::fstream::out);
        fs << "OK";
        fs.close();

        cout << "SIMULATION >> ID: " << mcParams[i].simulationID;
        cout << " (SEED:" << seed << "; THREAD:" << threadID << ")\n";
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        cout << "Simulation completed in " << duration.count() / 1000.0 << "s.\n" << endl;
    }
}


/**
 * Market Simulator.
 */
int main(int argc, char** argv) {

    if (argc > 3)
        throw simuException();

    const string &SIM_NAME = "/" + (string) argv[1];

    const string &WORKFLOW = "../Workflow" + SIM_NAME;
    const string &RESULT_PATH = WORKFLOW + "/Results";

    bool force_sim = argc == 3 && (string) argv[2] == "True";
    if (!force_sim) {
        ifstream f(RESULT_PATH + "/status.txt");
        if (f.good()) {
            std::string line;
            if (getline(f, line) && line == "OK") {
                force_sim = true;
            }
        }
    }

    // Start a new simulation if status is OK.
    boost::filesystem::path dir(RESULT_PATH);
    if (!boost::filesystem::exists(dir)) {
        boost::filesystem::create_directory(dir);
    } else {
        if (force_sim) {
            for (const auto &entry : boost::filesystem::directory_iterator(RESULT_PATH))
                boost::filesystem::remove_all(entry.path());
        }
    }

    std::cout << "--- MARKET SIMULATION ---" << std::endl;

    boost::property_tree::ptree psim;
    boost::property_tree::read_json(WORKFLOW + "/sim_params.json", psim);

    boost::property_tree::ptree pmodel;
    boost::property_tree::read_json(WORKFLOW + "/model_params.json", pmodel);

    boost::property_tree::ptree pfv;
    boost::property_tree::read_json(WORKFLOW + "/fundamental_value_params.json",pfv);

    auto symbol = psim.get<string>("symbol");
    auto n_threads = psim.get<int>("n_threads", 6);

    auto step_size = psim.get<unsigned long long>("step_size");
    auto date = psim.get<string>("date");

    auto n_mins = psim.get<long>("n_mins", 510);

    sim_params simParams = {
            .symbol = symbol,
            .closing_bid_prc = psim.get<double>("closing_bid_prc"),
            .closing_ask_prc = psim.get<double>("closing_ask_prc"),
            .tick_size = psim.get<float>("tick_size"),
            .n_steps = n_mins * 60000000ULL / step_size,
            // .n_steps = (unsigned long int) (n_mins * 60e6 / (double)step_size),
            .step_size = step_size,
            .n_threads = n_threads,
            .verbose = psim.get<int>("verbose"),
            .l2_depth = (unsigned int) psim.get<double>("l2_depth", 10),
            .tick_format = psim.get<string>("tick_format", ""),
            .date_format = psim.get<string>("date_format", "%d-%b-%Y %H:%M:%S"),
            .n_runs = psim.get<int>("n_runs", 1),
            .seed = psim.get<long>("seed"),
            .n_zi_traders = psim.get<unsigned int>("n_zi_traders", 100),
            .n_ft_traders = psim.get<unsigned int>("n_ft_traders", 0),
            .n_mt_traders = psim.get<unsigned int>("n_mt_traders", 0),
            .n_hmt_traders = psim.get<unsigned int>("n_hmt_traders", 0),
            .n_mm_traders = psim.get<unsigned int>("n_mm_traders", 0),
            .n_ins_traders = psim.get<unsigned int>("n_ins_traders", 0),
            .n_st_traders = psim.get<unsigned int>("n_st_traders", 0),
    };
    unsigned long int max_unsigned_long = std::numeric_limits<unsigned long>::max();
    // Hard-coded fundamental value parameters
    float fv0 = 0.5f * (float)simParams.closing_bid_prc + 0.5f * (float)simParams.closing_ask_prc;

    std::default_random_engine generator(simParams.seed);

    auto *seed_dist = new std::uniform_int_distribution<int>(0, INT_MAX);

    auto mean_child = pmodel.get_optional<double>("ZI_base_params.mean");
    auto *mean_dist = new std::uniform_real_distribution<double>(
            mean_child ? mean_child.value() : pmodel.get<double>("ZI_base_params.mean_min"),
            mean_child ? mean_child.value() : pmodel.get<double>("ZI_base_params.mean_max"));

    auto sd_child = pmodel.get_optional<double>("ZI_base_params.sd");
    auto *sd_dist = new std::uniform_real_distribution<double>(
            sd_child ? sd_child.value() : pmodel.get<double>("ZI_base_params.sd_min"),
            sd_child ? sd_child.value() : pmodel.get<double>("ZI_base_params.sd_max"));
    // use lambda when use exponential distribution
    /* auto lambda_child = pmodel.get_optional<double>("ZI_base_params.lambda");
    auto *lambda_dist = new std::uniform_real_distribution<double>(
            lambda_child ? lambda_child.value() : pmodel.get<double>("ZI_base_params.lambda_min"),
            lambda_child ? lambda_child.value() : pmodel.get<double>("ZI_base_params.lambda_max")); */

    std::uniform_real_distribution<double> * kappa_lo_dist;
    std::uniform_real_distribution<double> * kappa_lo_3_dist;
    double kappa_mo_ratio;
    int ft_freq;
    if(simParams.n_ft_traders > 0) {
        auto kappa_lo_child = pmodel.get_optional<double>("FT_params.kappa_lo");
        kappa_lo_dist = new std::uniform_real_distribution<double>(
                kappa_lo_child ? kappa_lo_child.value() : pmodel.get<double>("FT_params.kappa_lo_min"),
                kappa_lo_child ? kappa_lo_child.value() : pmodel.get<double>("FT_params.kappa_lo_max"));

        auto kappa_lo_3_child = pmodel.get_optional<double>("FT_params.kappa_lo_3");
        kappa_lo_3_dist = new std::uniform_real_distribution<double>(
                kappa_lo_3_child ? kappa_lo_3_child.value() : pmodel.get<double>("FT_params.kappa_lo_3_min"),
                kappa_lo_3_child ? kappa_lo_3_child.value() : pmodel.get<double>("FT_params.kappa_lo_3_max"));

        kappa_mo_ratio = pmodel.get<double>("FT_params.kappa_mo_ratio");
        ft_freq = pmodel.get<int>("FT_params.ft_freq");
    }

    std::uniform_real_distribution<double> * alpha_dist;
    std::uniform_real_distribution<double> * mt_beta_lo_dist;
    double beta_mo_ratio;
    double mt_gamma;
    if(simParams.n_mt_traders > 0) {
        auto alpha_child = pmodel.get_optional<double>("MT_params.alpha");
        alpha_dist = new std::uniform_real_distribution<double>(
                alpha_child ? alpha_child.value() : pmodel.get<double>("MT_params.alpha_min"),
                alpha_child ? alpha_child.value() : pmodel.get<double>("MT_params.alpha_max"));

        auto beta_lo_child = pmodel.get_optional<double>("MT_params.beta_lo");
        mt_beta_lo_dist = new std::uniform_real_distribution<double>(
                beta_lo_child ? beta_lo_child.value() : pmodel.get<double>("MT_params.beta_lo_min"),
                beta_lo_child ? beta_lo_child.value() : pmodel.get<double>("MT_params.beta_lo_max"));

        beta_mo_ratio = pmodel.get<double>("MT_params.beta_mo_ratio");
        mt_gamma = pmodel.get<double>("MT_params.gamma");
    }
    // Read high frequency momentum trader params
    std::uniform_real_distribution<double> * halpha_dist;
    std::uniform_real_distribution<double> * hmt_beta_lo_dist;
    double hbeta_mo_ratio;
    double hmt_gamma;
    if(simParams.n_hmt_traders > 0) {
        cout << "begin reading high frequency momentum trader params" << endl;
        auto halpha_child = pmodel.get_optional<double>("HMT_params.halpha");
        halpha_dist = new std::uniform_real_distribution<double>(
                halpha_child ? halpha_child.value() : pmodel.get<double>("HMT_params.halpha_min"),
                halpha_child ? halpha_child.value() : pmodel.get<double>("HMT_params.halpha_max"));

        auto hbeta_lo_child = pmodel.get_optional<double>("HMT_params.hbeta_lo");
        hmt_beta_lo_dist = new std::uniform_real_distribution<double>(
                hbeta_lo_child ? hbeta_lo_child.value() : pmodel.get<double>("HMT_params.hbeta_lo_min"),
                hbeta_lo_child ? hbeta_lo_child.value() : pmodel.get<double>("HMT_params.hbeta_lo_max"));

        hbeta_mo_ratio = pmodel.get<double>("HMT_params.hbeta_mo_ratio");
        hmt_gamma = pmodel.get<double>("HMT_params.hgamma");
    }
    // Finish reading high frequency momentum trader params

    // Read market maker parameters
    double mm_delta, mm_lo, mm_mo;
    unsigned int mm_vol, mm_mkvol;
    int mm_edge, mm_pos_limit, mm_pos_safe;
    long mm_rest;
    if (simParams.n_mm_traders > 0){
        cout << "begin reading market maker params" << endl;
        mm_delta = pmodel.get<double>("MM_params.mm_delta");
        mm_lo = pmodel.get<double>("MM_params.mm_lo");
        mm_mo = pmodel.get<double>("MM_params.mm_mo");
        mm_vol = pmodel.get<unsigned int>("MM_params.mm_vol");
        mm_edge = pmodel.get<int>("MM_params.mm_edge");
        mm_pos_limit = pmodel.get<int>("MM_params.mm_pos_limit");
        mm_pos_safe = pmodel.get<int>("MM_params.mm_pos_safe");
        mm_mkvol = pmodel.get<unsigned int>("MM_params.mm_mkvol");
        mm_rest = pmodel.get<long>("MM_params.mm_rest");
    }
    // Finish reading market maker parameters
    // Start reading INS parameters
    int ins_mode, total_vol;
    float ins_pov;
    unsigned long long start_step;
    unsigned int ins_vol, ins_interval, obs_interval;
    if (simParams.n_ins_traders > 0) {
        cout << "begin reading INS trader params" << endl;
        ins_mode = pmodel.get<int>("INS_params.ins_mode");
        ins_pov = pmodel.get<float>("INS_params.ins_pov");
        start_step = pmodel.get<unsigned long long>("INS_params.start_step");
        total_vol = pmodel.get<int>("INS_params.total_vol");
        ins_vol = pmodel.get<unsigned int>("INS_params.ins_vol");
        ins_interval = pmodel.get<unsigned int>("INS_params.ins_interval");
        obs_interval = pmodel.get<unsigned int>("INS_params.obs_interval");
    }
    // Finish reading INS parameters
    // Start reading spike trader parameters
    double st_mo;
    unsigned int st_vol;
    int st_interval;
    if (simParams.n_st_traders > 0) {
        st_mo = pmodel.get<double>("ST_params.st_mo");
        st_vol = pmodel.get<unsigned int>("ST_params.st_vol");
        st_interval = pmodel.get<int>("ST_params.st_interval");
    }
    // Finish reading spike trader parameters

    std::default_random_engine generator_calibration((long) (*seed_dist)(generator));

    auto delta = pmodel.get<double>("ZI_base_params.delta");
    auto limit_vol = pmodel.get<unsigned int>("ZI_base_params.limit_vol", 1);
    auto market_vol = pmodel.get<unsigned int>("ZI_base_params.market_vol", 1);

    auto zi_alpha = pmodel.get<double>("ZI_params.alpha");
    auto zi_mo_ratio = pmodel.get<double>("ZI_params.zi_mo_ratio");
    auto zi_mu = zi_alpha * zi_mo_ratio;

    auto ft_params_mu = pfv.get<float>("mu");
    auto ft_params_sigma = pfv.get<float>("sigma");
    bool ft_params_is_ready = pfv.get<int>("is_ready");
    auto ft_params_dump_freq = pfv.get<int>("dump_freq", 1);

    vector<mc_params> mcParamsVec;

    for (int run = 0; run < simParams.n_runs; ++run) {
        string run_str = to_string(run);

        long run_seed = (long) (*seed_dist)(generator);

        // double lambda = (*lambda_dist)(generator_calibration);
        double mean = (*mean_dist)(generator_calibration);
        double sd = (*sd_dist)(generator_calibration);

        zi_base_params ziBaseParams = {
                .delta = delta,
                .mean = mean,
                .sd = sd,
                .limit_vol = limit_vol,
                .market_vol = market_vol,
        };

        mc_params mcParams = {
                .simulationID = run,
                .seed = run_seed,
                .date = date,
                .results_path = WORKFLOW + "/Results" + "/run_" +
                        std::string(5 - run_str.size(), '0').append(run_str),
                .ziParams = {
                    .alpha = zi_alpha / simParams.n_zi_traders,
                    .mu = zi_mu /  simParams.n_zi_traders,
                    .ziBaseParams = ziBaseParams
                            },
                .fvParams = {
                        .s0 = fv0,
                        .mu = ft_params_mu,
                        .sigma = ft_params_sigma,
                        .step_size = step_size,
                        .seed = run_seed,
                        .dump_freq = ft_params_dump_freq,
                        .is_ready = ft_params_is_ready,
                        .source_path = WORKFLOW
                }
        };

        boost::property_tree::ptree p;
        p.put("sim_params.seed", run_seed);
        p.put("ZI_base_params.delta", delta);
        // p.put("ZI_base_params.lambda", lambda);
        p.put("ZI_base_params.mean", mean);
        p.put("ZI_base_params.sd", sd);
        p.put("ZI_params.alpha", zi_alpha);
        p.put("ZI_params.mu", zi_mu);

        if(simParams.n_ft_traders > 0) {
            auto ft_kappa_lo = (*kappa_lo_dist)(generator_calibration);
            auto ft_kappa_mo = ft_kappa_lo * kappa_mo_ratio;

            auto ft_kappa_lo_3 = (*kappa_lo_3_dist)(generator_calibration);
            auto ft_kappa_mo_3 = kappa_mo_ratio * ft_kappa_lo_3;

            mcParams.ftParams = {
                    .kappa_lo = ft_kappa_lo / simParams.n_ft_traders,
                    .kappa_mo = ft_kappa_mo / simParams.n_ft_traders,
                    .kappa_lo_3 = ft_kappa_lo_3 / simParams.n_ft_traders,
                    .kappa_mo_3 = ft_kappa_mo_3 / simParams.n_ft_traders,
                    .ft_freq = ft_freq,
                    .ziBaseParams = ziBaseParams
            };

            p.put("FT_params.kappa_mo", ft_kappa_mo);
            p.put("FT_params.kappa_lo", ft_kappa_lo);
            p.put("FT_params.kappa_mo_3", ft_kappa_mo_3);
            p.put("FT_params.kappa_lo_3", ft_kappa_lo_3);
            p.put("FT_params.ft_freq", ft_freq);
        }

        if(simParams.n_mt_traders > 0) {
            auto mt_beta_lo = (*mt_beta_lo_dist)(generator_calibration);
            auto mt_beta_mo = mt_beta_lo * beta_mo_ratio;
            auto mt_alpha = (*alpha_dist)(generator_calibration);

            mcParams.mtParams = {
                    .alpha = mt_alpha,
                    .beta_lo = mt_beta_lo / simParams.n_mt_traders,
                    .beta_mo = mt_beta_mo / simParams.n_mt_traders,
                    .gamma = mt_gamma,
                    .ziBaseParams = ziBaseParams
            };

            p.put("MT_params.beta_mo", mt_beta_mo);
            p.put("MT_params.beta_lo", mt_beta_lo);
            p.put("MT_params.gamma", mt_gamma);
            p.put("MT_params.alpha", mt_alpha);
        }
        // Add high frequency momentum traders
        if(simParams.n_hmt_traders > 0) {
            auto hmt_beta_lo = (*hmt_beta_lo_dist)(generator_calibration);
            auto hmt_beta_mo = hmt_beta_lo * hbeta_mo_ratio;
            auto hmt_alpha = (*halpha_dist)(generator_calibration);

            mcParams.hmtParams = {
                    .halpha = hmt_alpha,
                    .hbeta_lo = hmt_beta_lo / simParams.n_hmt_traders,
                    .hbeta_mo = hmt_beta_mo / simParams.n_hmt_traders,
                    .hgamma = hmt_gamma,
                    .ziBaseParams = ziBaseParams
            };

            p.put("HMT_params.hbeta_mo", hmt_beta_mo);
            p.put("HMT_params.hbeta_lo", hmt_beta_lo);
            p.put("HMT_params.hgamma", hmt_gamma);
            p.put("HMT_params.halpha", hmt_alpha);
        }
        // finish adding high frequency momentum traders
        //Add market makers
        if (simParams.n_mm_traders > 0){
            cout << "begin writing market maker params" << endl;
            mcParams.mmParams = {
                    .mm_delta = mm_delta,
                    .mm_lo = mm_lo / simParams.n_mm_traders,
                    .mm_mo = mm_mo / simParams.n_mm_traders,
                    .mm_vol = mm_vol,
                    .mm_edge = mm_edge,
                    .mm_pos_limit = mm_pos_limit,
                    .mm_pos_safe = mm_pos_safe,
                    .mm_mkvol = mm_mkvol,
                    .mm_rest = mm_rest
            };
            p.put("MM_params.mm_delta", mm_delta);
            p.put("MM_params.mm_lo", mm_lo);
            p.put("MM_params.mm_mo", mm_mo);
            p.put("MM_params.mm_vol", mm_vol);
            p.put("MM_params.mm_edge", mm_edge);
            p.put("MM_params.mm_pos_limit", mm_pos_limit);
            p.put("MM_params.mm_pos_safe", mm_pos_safe);
            p.put("MM_params.mm_mkvol", mm_mkvol);
            p.put("MM_params.mm_rest", mm_rest);
        }
        //finish adding market makers

        //Add INS trader
        if (simParams.n_ins_traders > 0) {
            cout << "begin writing INS trader params" << endl;
            mcParams.insParams = {
                    .ins_mode = ins_mode,
                    .ins_pov = ins_pov,
                    .start_step = start_step,
                    .total_vol = total_vol,
                    .ins_vol = ins_vol,
                    .ins_interval = ins_interval,
                    .obs_interval = obs_interval
            };
            p.put("INS_params.ins_mode", ins_mode);
            p.put("INS_params.ins_pov", ins_pov);
            p.put("INS_params.start_step", start_step);
            p.put("INS_params.total_vol", total_vol);
            p.put("INS_params.ins_vol", ins_vol);
            p.put("INS_params.ins_interval", ins_interval);
            p.put("INS_params.obs_interval", obs_interval);
        }
        // Finish adding INS trader

        // Add spike trader
        if (simParams.n_st_traders > 0) {
            mcParams.stParams = {
                    .st_mo = st_mo,
                    .st_vol = st_vol,
                    .st_interval = st_interval
            };
            p.put("ST_params.st_mo", st_mo);
            p.put("ST_params.st_vol", st_vol);
            p.put("ST_params.st_interval", st_interval);
        }
        // Finish adding spike trader

        boost::filesystem::path dir(mcParams.results_path);
        boost::filesystem::create_directory(dir);
        boost::property_tree::write_json(mcParams.results_path + "/run_params.json", p);

        mcParamsVec.push_back(mcParams);
    }

    unsigned int _n_threads = min(min(simParams.n_threads, simParams.n_runs), (int)std::thread::hardware_concurrency());

    auto start = std::chrono::high_resolution_clock::now();
    cout << "MONTE-CARLO SIMULATION (N_THREADS:" << _n_threads << ", MC_RUNS:" << simParams.n_runs << "):\n\n";

    std::vector<mc_params> _mcParams[_n_threads];
    for (int i = 0; i < simParams.n_runs; ++i)
        _mcParams[i % _n_threads].push_back(mcParamsVec[i]);

    std::thread threadPool[_n_threads];
    for (int i = 0; i < _n_threads; ++i)
        threadPool[i] = std::thread(run_task, i, simParams, _mcParams[i]);

    for (int i = 0; i < _n_threads; ++i)
        threadPool[i].join();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    cout << "Monte-carlo simulation completed in " << duration.count() / 1000.0 << "s." << endl;

    fstream fs;
    fs.open(RESULT_PATH + "/status.txt", std::fstream::out);
    fs << "OK";
    fs.close();
}
