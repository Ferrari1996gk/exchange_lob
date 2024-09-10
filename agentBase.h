//
// Created by Perukrishnen Vytelingum on 29/07/2020.
//

#ifndef ZEROINTELLIGENCEMARKET_AGENTBASE_H
#define ZEROINTELLIGENCEMARKET_AGENTBASE_H

#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

#include <random>
#include <chrono>
#include <iostream>
#include <utility>

#include "cda_engine.h"
#include "params.h"

const int ALPHA = 0;
const int MU = 1;
const int DELTA = 2;
const int DP = 3;

/**
 * Base agent class modelling a trader, each with an independent random number generator.
 */
class agentBase {

public:
    agentBase(long seed, const string &agentID);

    virtual void get_orders(deque<lob_order> *orders, L1 * l1) = 0;

    virtual string get_traderType() = 0;

    virtual int get_traderTypeNum() = 0;

    virtual void handle_execution_report(execution_report *report) {
        // Do nothing by default.
    };

    virtual void handle_l1_report(const L1 *l1) {
        // Do nothing by default.
    }

    virtual void handle_trade_report(const Trade *trade) {
        // Do nothing by default.
    }

    virtual void update_fundamental_value(float fvalue) {
        // Do nothing by default.
    }

    virtual double get_momentum(){
        // Return 0 by default.
        return 0.;
    }

    virtual void update_position(execution_report *report){
        if (report->executionReportType != ExecutionReportType::FILL &&
            report->executionReportType != ExecutionReportType::PARTIAL_FILL){
            throw simuException();
        }

        if (report->side == Side::BUY) {
            position += report->executedQuantity;
        } else{
            position -= report->executedQuantity;
        }
    }

    int get_position() const{
        return position;
    }

    void set_position(int init_position) {
        position = init_position;
    }

    virtual bool require_fundamental_value() {
        return false;
    }

    string agentID;
    int agent_index;
    int position = 0;

protected:
    std::default_random_engine generator;

    unsigned long int sequence = 0;

    lob_order get_cancel_order(const lob_order& queued_order, unsigned long int step, string time) {
        lob_order _ord = {
                .prc=queued_order.prc, .vol=queued_order.vol, .order_id=queued_order.order_id, .agent_id=agentID,
                .agent_index=agent_index, .t=(double) step, .side=queued_order.side, .orderType=OrdType::CANCEL,
                .time=std::move(time), .traderType=get_traderType()
        };

        return _ord;
    }

    lob_order get_limit_order(unsigned int prc, unsigned int vol, string order_id, unsigned long int step,
                              string time, Side side, string msg) {

        assert(1000000 > vol > 0);

        lob_order _ord = {.prc=prc, .vol=vol, .order_id=std::move(order_id), .agent_id=agentID,
                          .agent_index=agent_index, .t=(double) step, .expiry=(double) CLOSE, .side=side,
                          .orderType=OrdType::LIMIT, .time=std::move(time), .traderType=get_traderType(),
                          .message = msg,
        };

        return _ord;
    }

    lob_order get_market_order(unsigned int vol, string order_id, unsigned long int step,
                               string time, Side side, string msg) {

        assert(1000000 > vol > 0);

        lob_order _ord = {.prc=0, .vol=vol, .order_id=std::move(order_id), .agent_id=agentID, .agent_index=agent_index,
                          .t=(double) step, .expiry=(double) step, .side=side, .orderType=OrdType::MARKET,
                          .time=std::move(time), .traderType=get_traderType(), .message = msg,
        };

        return _ord;
    }

    lob_order get_amend_order(unsigned int prc, unsigned int vol, string order_id, unsigned long int step,
                              string time, Side side, string msg) {

        assert(vol > 0);

        lob_order _ord = {.prc=prc, .vol=vol, .order_id=std::move(order_id), .agent_id=agentID,
                          .agent_index=agent_index, .t=(double) step, .expiry=(double) CLOSE, .side=side,
                          .orderType=OrdType::AMEND, .time=std::move(time), .traderType=get_traderType(),
                          .message = msg,
        };

        return _ord;
    }

    unsigned long int CLOSE = 30600000;

    int PRIMARY = 0;

    std::uniform_real_distribution<double> *uniform_dist = new std::uniform_real_distribution<double>(0.0, 1.0);
};


class gatewayAgentBase : public agentBase {
public:
    gatewayAgentBase(long seed, const string &agentID) : agentBase(seed, agentID) {}

    void handle_execution_report(execution_report *report) override {
        if (report->executionReportType == ExecutionReportType::NEW)
            handle_new_report(report);
        else if (report->executionReportType == ExecutionReportType::AMEND)
            handle_amend_report(report);
        else if (report->executionReportType == ExecutionReportType::CANCEL)
            handle_cancel_report(report);
        else if (report->executionReportType == ExecutionReportType::FILL)
            handle_fill_report(report);
        else if (report->executionReportType == ExecutionReportType::PARTIAL_FILL)
            handle_partial_fill_report(report);
        else if (report->executionReportType == ExecutionReportType::REJECTED)
            handle_rejected_report(report);
        else if (report->executionReportType == ExecutionReportType::EXPIRED)
            handle_expired_report(report);
    };

    virtual void handle_new_report(execution_report *report);

    virtual void handle_amend_report(execution_report *report);

    virtual void handle_cancel_report(execution_report *report);

    virtual void handle_fill_report(execution_report *report);

    virtual void handle_partial_fill_report(execution_report *report);

    virtual void handle_rejected_report(execution_report *report);

    virtual void handle_expired_report(execution_report *report) {}

protected:
    deque<lob_order> queued_orders;
};


/**
 * Base Zero-Intelligence trader with alpha, mu and delta parameters for Buy and Sell.
 */
class ZiBaseTrader : public gatewayAgentBase {
public:
    ZiBaseTrader(long seed, const string &agentID, zi_base_params params);

    void get_orders(deque<lob_order> *orders, L1 * l1) override;

    unsigned int get_prc(Side side, L1 * l1);

    unsigned int get_limit_vol() {
        return ziBaseParams.limit_vol;
    };

    unsigned int get_market_vol() {
        return ziBaseParams.market_vol;
    };

    double get_delta() const;

    void submit_limit_orders(deque<lob_order> *orders, L1 * l1);

    void submit_market_orders(deque<lob_order> *orders, L1 * l1);


    virtual double get_alpha() = 0;

    virtual double get_mu() = 0;

    virtual Side get_side() = 0;

    void cancel_orders(deque<lob_order> *orders, L1 * l1);

    virtual string get_message() {
        return " ";
    }

protected:
    std::default_random_engine generators[4];

private:
    std::lognormal_distribution<double> *dp_dist;

    zi_base_params ziBaseParams{};
};

#endif //ZEROINTELLIGENCEMARKET_AGENTBASE_H
