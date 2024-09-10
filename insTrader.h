//
// Created by kgao_smd on 21/01/2022.
//

#ifndef SIMULATEDMARKET_INSTRADER_H
#define SIMULATEDMARKET_INSTRADER_H

#include "agentBase.h"
#include <boost/circular_buffer.hpp>
#include <numeric>
#include <memory>
#include <algorithm>

class insTrader : public gatewayAgentBase {
public:
    insTrader(long seed, const string &agentID, const ins_params& insParams);

    void get_orders(deque<lob_order> *orders, L1 * l1) override;

    void submit_market_orders(deque<lob_order> *orders, L1 * l1);


    static string get_message(){
        return " ";
    }

    string get_traderType() override{
        return "INS";
    }
    int get_traderTypeNum() override {
        return 6;
    }

    void handle_l1_report(const L1* l1) override;

    void handle_trade_report(const Trade *trade) override;

//protected:
//    std::default_random_engine  generators[4];

private:
    ins_params insParams;
    bool ins_active = false;
    std::unique_ptr<boost::circular_buffer<unsigned int>> vol_ptr;
    unsigned int ins_vol;
    float interval_ratio;


    unsigned int get_market_vol() const {
        return ins_vol;
    };

};


#endif //SIMULATEDMARKET_INSTRADER_H
