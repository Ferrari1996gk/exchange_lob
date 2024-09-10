//
// Created by Anna on 02/03/2021.
//

#ifndef ZEROINTELLIGENCEMARKET_FUNDAMENTALTRADER_H
#define ZEROINTELLIGENCEMARKET_FUNDAMENTALTRADER_H

#include "agentBase.h"


class fundamentalTrader: public ZiBaseTrader {
public:
    fundamentalTrader(long seed, const string & agentID,
                      const ft_params& ftParams, float tick_size);

    string get_traderType() override {
        return "FT";
    }
    void get_orders(deque<lob_order> *orders, L1 * l1) override{
        if (l1->step % ftParams.ft_freq == 0){
            cancel_orders(orders, l1);
            // submit_limit_orders(orders, l1);
            submit_market_orders(orders, l1);
        }
    }

    int get_traderTypeNum() override {
        return 1;
    }
    void handle_trade_report(const Trade* trade) override;
    void handle_l1_report(const L1* l1) override;
    Side get_side() override;
    double get_alpha() override;
    double get_mu() override;

    string get_message() override {
        return std::to_string(price_distortion);
    }

    void update_fundamental_value(float fv) override {
        fundamental_value = fv;
    }

    bool require_fundamental_value() override {
        return true;
    }

private:
    ft_params ftParams;
    float tick_size;
    float fundamental_value = 0.0F;
    float price_distortion = 0.0F;
};


#endif //ZEROINTELLIGENCEMARKET_FUNDAMENTALTRADER_H
