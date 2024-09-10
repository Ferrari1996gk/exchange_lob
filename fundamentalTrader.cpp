//
// Created by Anna on 03/03/2021.
//

#include "fundamentalTrader.h"

fundamentalTrader::fundamentalTrader(long seed, const string &agentID, const ft_params& ftParams,
                                     float tick_size) :
                                     ZiBaseTrader(seed, agentID, ftParams.ziBaseParams) {
    fundamentalTrader::ftParams = ftParams;
    fundamentalTrader::tick_size = tick_size;
}


void fundamentalTrader::handle_trade_report(const Trade* trade) {
    gatewayAgentBase::handle_trade_report(trade);
    price_distortion = fundamental_value == 0.0F ? 0.0F : fundamental_value - trade->vwap;
}


void fundamentalTrader::handle_l1_report(const L1* l1) {
    gatewayAgentBase::handle_l1_report(l1);
    if (fundamental_value == 0.0F) {
        price_distortion = 0.0F;
        return;
    }

    float l1a = (float)l1->best_ask_prc * tick_size;
    float l1b = (float)l1->best_bid_prc * tick_size;
    if (fundamental_value > l1a) {
        price_distortion = fundamental_value - l1a;
    } else if (fundamental_value < l1b) {
        price_distortion = fundamental_value - l1b;
    } else {
        price_distortion = 0.0F;
    }
}


Side fundamentalTrader::get_side() {
    return price_distortion > 0 ? Side::BUY : Side::SELL;
}


double fundamentalTrader::get_alpha() {
    double p = abs(price_distortion / fundamental_value) * 100;
    return ftParams.kappa_lo * p + ftParams.kappa_lo_3 * pow(p,3);
}


double fundamentalTrader::get_mu() {
    double p = abs(price_distortion / fundamental_value) * 100;
    return ftParams.kappa_mo * p + ftParams.kappa_mo_3 * pow(p,3);
}

