//
// Created by Anna on 26/03/2021.
//

#include "ziTrader.h"

/**
 * Preis Base Zero-Intelligence Trader with alpha, mu and delta parameters for Buy and Sell.
 * @param seed
 * @param ziParams
 */
ZiTrader::ZiTrader(long seed, const string & agentID, zi_params ziParams) : ZiBaseTrader(
        seed, agentID, ziParams.ziBaseParams) {

    ZiTrader::ziParams = ziParams;

    side_dist = new std::uniform_real_distribution<double>(0.0, 1.0);
}

Side ZiTrader::get_side() {
    return (*side_dist)(generators[PRIMARY]) < 0.5 ? Side::BUY : Side::SELL;
}

double ZiTrader::get_alpha() {
    return ziParams.alpha;
}

double ZiTrader::get_mu() {
    return ziParams.mu;
}