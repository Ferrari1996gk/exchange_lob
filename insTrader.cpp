//
// Created by kgao_smd on 21/01/2022.
//
#include "insTrader.h"

insTrader::insTrader(long seed, const string &agentID, const ins_params &insParams) :
        gatewayAgentBase::gatewayAgentBase(seed, agentID){
    insTrader::insParams = insParams;
    set_position(insParams.total_vol);
    if (insParams.ins_mode == 0) {
        ins_vol = insParams.ins_vol;
    } else {
        ins_vol = 0;
        interval_ratio = insParams.ins_interval / (float) insParams.obs_interval;
        vol_ptr = std::make_unique<boost::circular_buffer<unsigned int>>(insParams.obs_interval);
        cout << "interval_ratio: " << interval_ratio << endl;
        cout << vol_ptr->size() << " " << vol_ptr->capacity() << endl;
    }

}

void insTrader::handle_l1_report(const L1 *l1) {
    if (l1->step >= insParams.start_step && position >= 0 && l1->step % insParams.ins_interval == 0)
        ins_active = true;
    else
        ins_active = false;

    gatewayAgentBase::handle_l1_report(l1);
}

void insTrader::handle_trade_report(const Trade *trade) {
    agentBase::handle_trade_report(trade);
    if (insParams.ins_mode != 0) {
        vol_ptr->push_back(trade->vol);
        ins_vol = std::max<unsigned int>(1, std::accumulate(vol_ptr->begin(),vol_ptr->end(),
                                                            (unsigned int)0) * insParams.ins_pov * interval_ratio);
    }
}

void insTrader::submit_market_orders(deque<lob_order> *orders, L1 * l1) {

    unsigned long int step = l1->step;
    const string &time = l1->time;

    Side side = Side::SELL;
    unsigned int vol = get_market_vol();
    string order_id = agentID + "-m-" + std::to_string(sequence++);
    lob_order _ord = get_market_order(vol, order_id, step, time, side, get_message());

    orders->push_back(_ord);

}


void insTrader::get_orders(deque<lob_order> *orders, L1 * l1) {
    if (ins_active){
        submit_market_orders(orders, l1);
    }
}
