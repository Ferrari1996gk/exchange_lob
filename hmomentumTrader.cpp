//
// Created by kgao_smd on 22/08/2021.
//
#include "hmomentumTrader.h"

hmomentumTrader::hmomentumTrader(long seed, const string &agentID, const hmt_params& hmtParams, float tick_size) :
        ZiBaseTrader(seed, agentID, hmtParams.ziBaseParams) {
    hmomentumTrader::hmtParams = hmtParams;
    hmomentumTrader::tick_size = tick_size;
}


void hmomentumTrader::handle_trade_report(const Trade* trade) {
    gatewayAgentBase::handle_trade_report(trade);
    update_momentum(trade->vwap);
}


void hmomentumTrader::handle_l1_report(const L1 *l1)  {
    gatewayAgentBase::handle_l1_report(l1);
    update_momentum(l1->get_mid_prc() * tick_size);
}
