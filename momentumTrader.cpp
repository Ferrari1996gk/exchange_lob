//
// Created by Anna on 09/03/2021.
//
#include "momentumTrader.h"

momentumTrader::momentumTrader(long seed, const string &agentID, const mt_params& mtParams, float tick_size) :
                               ZiBaseTrader(seed, agentID, mtParams.ziBaseParams) {
    momentumTrader::mtParams = mtParams;
    momentumTrader::tick_size = tick_size;
}


void momentumTrader::handle_trade_report(const Trade* trade) {
    gatewayAgentBase::handle_trade_report(trade);
    update_momentum(trade->vwap);
}


void momentumTrader::handle_l1_report(const L1 *l1)  {
    gatewayAgentBase::handle_l1_report(l1);
    update_momentum(l1->get_mid_prc() * tick_size);
}
