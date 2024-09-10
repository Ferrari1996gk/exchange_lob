//
// Created by Anna on 09/03/2021.
//

#ifndef ZEROINTELLIGENCEMARKET_MOMENTUMTRADER_H
#define ZEROINTELLIGENCEMARKET_MOMENTUMTRADER_H

#include "agentBase.h"


class momentumTrader : public ZiBaseTrader {
public:
    momentumTrader(long seed, const string & agentID,
                   const mt_params& mtParams,
                   float tick_size);

    string get_traderType() override {
        return "MT";
    }

    void get_orders(deque<lob_order> *orders, L1 * l1) override{
        cancel_orders(orders, l1);
        submit_limit_orders(orders, l1);
        submit_market_orders(orders, l1);
    }

    int get_traderTypeNum() override {
        return 2;
    }

    double get_momentum() override {
        return momentum;
    }

    void handle_trade_report(const Trade* trade) override;
    void handle_l1_report(const L1* l1) override;

private:
    mt_params mtParams;
    float tick_size;
    double momentum = 0.0;
    float last_market_price = 0.0F;
    

    Side get_side() override {
        return momentum > 0 ? Side::BUY : Side::SELL;
    }

    double get_demand() {
        return tanh(abs(momentum) * mtParams.gamma);
    }

    double get_alpha() override {
        return mtParams.beta_lo * get_demand();
    };

    double get_mu() override {
        return mtParams.beta_mo * get_demand();
    }

    string get_message() override {
        return std::to_string(momentum);
    }

    void update_momentum(float price) {
        if (last_market_price != 0)
            momentum = mtParams.alpha * (price - last_market_price) + (1.0 - mtParams.alpha) * momentum;

        last_market_price = price;
    }
};

#endif //ZEROINTELLIGENCEMARKET_MOMENTUMTRADER_H
