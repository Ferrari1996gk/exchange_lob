//
// Created by Anna on 26/03/2021.
//

#ifndef ZEROINTELLIGENCEMARKET_ZITRADER_H
#define ZEROINTELLIGENCEMARKET_ZITRADER_H

#include "agentBase.h"

class ZiTrader : public ZiBaseTrader {
public:
    ZiTrader(long seed, const string &agentID, zi_params params);

    string get_traderType() override {
        return "ZI";
    }

    int get_traderTypeNum() override {
        return 0;
    }

    Side get_side() override;
    double get_alpha() override;
    double get_mu() override;

private:
    zi_params ziParams{};

    std::uniform_real_distribution<double> *side_dist;
};

#endif //ZEROINTELLIGENCEMARKET_ZITRADER_H
