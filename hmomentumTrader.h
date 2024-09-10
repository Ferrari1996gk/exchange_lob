//
// Created by kgao_smd on 22/08/2021.
//

#ifndef ZEROINTELLIGENCEMARKET_HMOMENTUMTRADER_H
#define ZEROINTELLIGENCEMARKET_HMOMENTUMTRADER_H

#include "agentBase.h"


class hmomentumTrader : public ZiBaseTrader {
public:
    hmomentumTrader(long seed, const string & agentID,
                    const hmt_params& hmtParams,
                    float tick_size);

    string get_traderType() override {
        return "HMT";
    }
    int get_traderTypeNum() override {
        return 3;
    }

    double get_momentum() override {
        return momentum;
    }

    void handle_trade_report(const Trade* trade) override;
    void handle_l1_report(const L1* l1) override;

private:
    hmt_params hmtParams;
    float tick_size;
    double momentum = 0.0;
    float last_market_price = 0.0F;


    Side get_side() override {
        return momentum > 0 ? Side::BUY : Side::SELL;
    }

    double get_demand() {
        return tanh(abs(momentum) * hmtParams.hgamma);
    }

    double get_alpha() override {
        return hmtParams.hbeta_lo * get_demand();
    };

    double get_mu() override {
        return hmtParams.hbeta_mo * get_demand();
    }

    string get_message() override {
        return std::to_string(momentum);
    }

    void update_momentum(float price) {
        if (last_market_price != 0)
            momentum = hmtParams.halpha * (price - last_market_price) + (1.0 - hmtParams.halpha) * momentum;

        last_market_price = price;
    }
};

#endif //ZEROINTELLIGENCEMARKET_SPIKETRADER_H
