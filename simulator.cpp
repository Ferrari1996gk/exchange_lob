//
// Created by Perukrishnen Vytelingum on 03/08/2020.
//

#include "simulator.h"


simulator::simulator(sim_params * simParams, const mc_params& mcParams) {

    simulator::simParams = simParams;

    zi_params ziParams = mcParams.ziParams;
    ft_params ftParams = mcParams.ftParams;
    mt_params mtParams = mcParams.mtParams;
    hmt_params hmtParams = mcParams.hmtParams;
    mm_params mmParams = mcParams.mmParams;
    ins_params insParams = mcParams.insParams;
    st_params stParams = mcParams.stParams;

    float tick_size = simParams->tick_size;
    int reference_price[2] = {
            (int) round(simParams->closing_bid_prc / tick_size),
            (int) round(simParams->closing_ask_prc / tick_size)
    };

    simDate = get_time_point(mcParams.date, "%Y-%m-%d", false);
    string symbol = simParams->symbol;
    engine = new Engine(symbol, reference_price, tick_size, mcParams.results_path,
                        simParams->verbose, simParams->tick_format);

    generator.seed(mcParams.seed);
    fv = new fundamentalValue((*seed_dist)(generator), mcParams.fvParams, mcParams.results_path);
    total_steps = fv->ready()? std::min(fv->get_source_length(), simParams->n_steps) : simParams->n_steps;
    // momentum value
    mv = new momentumValue(mcParams.results_path);

    // Preis' Zero-Intelligence Traders.
    loadZITraders(simParams->n_zi_traders, ziParams);
    loadFTraders(simParams->n_ft_traders, ftParams, simParams->tick_size);
    loadMTraders(simParams->n_mt_traders, mtParams, simParams->tick_size);
    loadHMTraders(simParams->n_hmt_traders, hmtParams, simParams->tick_size);
    loadMarketMakers(simParams->n_mm_traders, mmParams, simParams->tick_size);
    loadInsTrader(simParams->n_ins_traders, insParams);
    loadSpikeTraders(simParams->n_st_traders, stParams);

    int index = 0;
    for (auto *a : agentPool)
        a->agent_index = index++;

    previousL1 = {.best_bid_prc=0, .best_ask_prc=0, .step=0};
    if (simParams->verbose >= 2){
        openPositionFs(mcParams.results_path);
    }
}


void simulator::run() {

    auto start_of_day = get_market_open();

    for(unsigned long int step = 0; step < total_steps; ++step) {
        string time = get_time_str(start_of_day, step, simParams->step_size, simParams->date_format);

        fv->update(step, time);
        update_fundamental_value(fv->get_value());
        calculate_total_momentum();
        mv->update(step, time, totalMomentum);

        L1 l1 = engine->get_l1(step, time);
        if (!(previousL1 == l1))
            previousL1 = l1;

        // TODO: shuffle agent pool using generator.
        deque<lob_order> orders;
        for (auto *a : agentPool)
            a->get_orders(&orders, &l1);

        deque<transaction> transactions;
        for (auto o : orders) {

            assert(o.agent_index >= 0 && o.agent_index < agentPool.size());
            deque<execution_report> reports;
            engine->process_order(o, &reports, &transactions);
            // Report execution to individual traders.
            if (!reports.empty()) {
                while (!reports.empty()) {
                    auto report = reports.front();
                    agentPool[report.agent_index]->handle_execution_report(&report);
                    reports.pop_front();
                }
            }
        }

        if (!transactions.empty()) {

            double sum_prc = 0;
            double total_vol = 0;

            for (const auto &tr : transactions) {
                sum_prc += tr.prc * tr.vol;
                total_vol += tr.vol;
            }

            // Report trade to all traders.
            if (total_vol > 0) {
                Trade trade = {
                        .vwap =  (float) (simParams->tick_size * sum_prc / total_vol),
                        .vol = (unsigned int) total_vol,
                        .step=step,
                        .time=time
                };

                for (auto *a : agentPool)
                    a->handle_trade_report(&trade);
            }
        }

        deque<execution_report> reports;
        bool has_expired = engine->expire((double) step, &reports);
        if (!reports.empty())
            for (auto report : reports)
                agentPool[report.agent_index]->handle_execution_report(&report);

        if (!orders.empty() || has_expired) {

            // Report L1 to all traders.
            L1 l1_ = engine->get_l1(step, time);
            if (!(l1_ == l1)) {
                for (auto *a : agentPool)
                    a->handle_l1_report(&l1_);
            }

            if (simParams->verbose >= 0)
                engine->save_l1((double) step, time);

            if (simParams->verbose >= 4)
                engine->save_l2((double) step, time, simParams->l2_depth);

            if (simParams->verbose >= 5)
                engine->save_l3((double) step, time);
        }
        if (fs_pos_open) {
            clearPosition();
            computePosition();
            outputPosition((double) step, time);
        }
    }

    fv->close();
    mv->close();
    if (fs_pos_open) {fs_pos.close();}

    cout << endl;
    cout << "Symbol                  = " << simParams->symbol << endl;
    cout << "Number of orders        = " << engine->sequence << endl;
    cout << "Number of transactions  = " << engine->numTransactions << endl;
    delete fv;
    delete mv;
    delete engine;
}


void simulator::loadZITraders(unsigned int n_traders, zi_params ziParams) {
    for(int i = 0; i < n_traders; ++i) {
        auto *trader = new ZiTrader((*seed_dist)(generator), "ZI|" + to_string(i), ziParams);
        agentPool.push_back(trader);
    }
}

void simulator::loadFTraders(unsigned int n_traders, const ft_params& ftParams, float tick_size) {
    for (int i = 0; i < n_traders; ++i) {
        auto *trader = new fundamentalTrader((*seed_dist)(generator),
                                             "FT|" + to_string(i),
                                             ftParams, tick_size);
        agentPool.push_back(trader);
    }
}

void simulator::loadMTraders(unsigned int n_traders, const mt_params& mtParams, float tick_size) {
    for (int i = 0; i < n_traders; ++i) {
        auto *trader = new momentumTrader((*seed_dist)(generator),
                                          "MT|" + to_string(i), mtParams, tick_size);
        agentPool.push_back(trader);
    }
}

void simulator::loadHMTraders(unsigned int n_traders, const hmt_params& hmtParams, float tick_size) {
    for (int i = 0; i < n_traders; ++i) {
        auto *trader = new hmomentumTrader((*seed_dist)(generator),
                                       "HMT|" + to_string(i), hmtParams, tick_size);
        agentPool.push_back(trader);
    }
}

void simulator::loadMarketMakers(unsigned int n_traders, const mm_params& mmParams, float tick_size) {
    for (int i = 0; i < n_traders; ++i) {
        auto *trader = new marketMaker((*seed_dist)(generator),
                                       "MM|" + to_string(i), mmParams, tick_size);
        agentPool.push_back(trader);
    }
}

void simulator::loadInsTrader(unsigned int n_traders, const ins_params& insParams) {
    for (int i = 0; i < n_traders; ++i) {
        auto *trader = new insTrader((*seed_dist)(generator),
                                       "INS|" + to_string(i), insParams);
        agentPool.push_back(trader);
    }
}

void simulator::loadSpikeTraders(unsigned int n_traders, const st_params &stParams) {
    for (int i = 0; i < n_traders; ++i) {
        auto *trader = new spikeTrader((*seed_dist)(generator),
                                     "ST|" + to_string(i), stParams);
        agentPool.push_back(trader);
    }
}


void simulator::update_fundamental_value(float fundamental_value) {
    for (auto& agent : agentPool) {
        if (agent->require_fundamental_value()) {
            agent->update_fundamental_value(fundamental_value);
        }
    }
}

void simulator::calculate_total_momentum() {
    totalMomentum = 0.;
    for (auto& agent : agentPool) {
        if (agent->get_traderType() == "MT") {
            totalMomentum += agent->get_momentum();
        }
    }
}

