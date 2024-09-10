//
// Created by kgao_smd on 10/02/2022.
//

#include "spikeTrader.h"

spikeTrader::spikeTrader(long seed, const string &agentID, const st_params &stParams) :
            gatewayAgentBase::gatewayAgentBase(seed, agentID){
    spikeTrader::stParams = stParams;
    side_dist = new std::uniform_real_distribution<double>(0.0, 1.0);

}

void spikeTrader::get_orders(deque<lob_order> *orders, L1 *l1) {
    if (st_active > 0){
        submit_market_orders(orders, l1);
        st_active -= 1;
    } else{
        double p = (*uniform_dist)(generators[ST_MU]);
        if (p < stParams.st_mo) {
            st_active = stParams.st_interval;
            st_side = (*side_dist)(generators[ST_SIDE]) < 0.5 ? Side::BUY : Side::SELL;
        }

    }

}

void spikeTrader::submit_market_orders(deque<lob_order> *orders, L1 *l1) {
    unsigned long int step = l1->step;
    const string &time = l1->time;

    unsigned int vol = get_market_vol();
    string order_id = agentID + "-m-" + std::to_string(sequence++);
    lob_order _ord = get_market_order(vol, order_id, step, time, st_side, get_message());

    orders->push_back(_ord);
}

void spikeTrader::handle_l1_report(const L1 *l1) {
    gatewayAgentBase::handle_l1_report(l1);
}

