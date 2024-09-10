//
// Created by kgao_smd on 10/02/2022.
//

#ifndef SIMULATEDMARKET_SPIKETRADER_H
#define SIMULATEDMARKET_SPIKETRADER_H

#include "agentBase.h"

const int ST_SIDE = 0;
const int ST_MU = 1;

class spikeTrader : public gatewayAgentBase {
public:
    spikeTrader(long seed, const string& agentID, const st_params& stParams);

    void get_orders(deque<lob_order> *orders, L1 * l1) override;

    void submit_market_orders(deque<lob_order> *orders, L1 *l1);

    static string get_message(){
        return " ";
    }

    string get_traderType() override {
        return "ST";
    }
    int get_traderTypeNum() override {
        return 5;
    }

    void handle_l1_report(const L1* l1) override;

protected:
    std::default_random_engine generators[2];

private:
    st_params stParams;
    int st_active = 0;
    Side st_side;
    std::uniform_real_distribution<double> *side_dist;

    unsigned int get_market_vol() const{
        return stParams.st_vol;
    }

};

#endif //SIMULATEDMARKET_SPIKETRADER_H
