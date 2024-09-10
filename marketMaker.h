//
// Created by kgao_smd on 19/01/2022.
//

#ifndef SIMULATEDMARKET_MARKETMAKER_H
#define SIMULATEDMARKET_MARKETMAKER_H

#include "agentBase.h"

const int BID_DP = 0;
const int ASK_DP = 1;
const int CANCEL = 2;
const int LIMIT = 3;

class marketMaker : public gatewayAgentBase{
public:
    marketMaker(long seed, const string & agentID, const mm_params& mmParams, float tick_size);

    void get_orders(deque<lob_order> *orders, L1 * l1) override;

    unsigned int get_bid_prc(L1* l1);
    unsigned int get_ask_prc(L1* l1);

    void submit_limit_orders(deque<lob_order> *orders, L1 * l1);

    void submit_market_orders(deque<lob_order> *orders, L1 *l1);

    void cancel_orders(deque<lob_order> *orders, L1 * l1);

    void cancel_all_orders(deque<lob_order> *orders, L1 * l1);

    static string get_message(){
        return " ";
    }

    string get_traderType() override{
        return "MM";
    }
    int get_traderTypeNum() override {
        return 4;
    }

    void handle_l1_report(const L1* l1) override;

protected:
    std::default_random_engine  generators[4];
private:
    mm_params mmParams{};
    float tick_size;
    float last_market_price = 0.0F;
    bool mo_flag = false;
    unsigned long long start_step = 0;

    double get_alpha() const{
        return mmParams.mm_lo;
    }

    double get_mu() const {
        return mmParams.mm_mo;
    }

    unsigned int get_limit_vol() const{
        return mmParams.mm_vol;
    }

    unsigned int get_market_vol() const{
        return mmParams.mm_mkvol;
    }

    double get_delta() const {
        return mmParams.mm_delta;
    }

};


#endif //SIMULATEDMARKET_MARKETMAKER_H
