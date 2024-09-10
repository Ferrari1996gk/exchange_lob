//
// Created by Perukrishnen Vytelingum on 29/07/2020.
//

#include "agentBase.h"
#include "params.h"

#include <utility>

/**
 * Base Agent for Market Simulation, with a single random number generator and a pointer to the CDA engine.
 * @param seed
 * @param agentID
 */
agentBase::agentBase(long seed, const string & agentID) {
    agentBase::agentID = agentID;
    generator.seed(seed);
}


/**
 * Base ZI Trader with alpha, mu and delta parameters for Buy and Sell.
 * @param seed
 * @param agentID
 */
ZiBaseTrader::ZiBaseTrader(long seed, const string & agentID, zi_base_params params) :
                            gatewayAgentBase::gatewayAgentBase(seed, agentID) {

    ziBaseParams = params;
    dp_dist = new std::lognormal_distribution<double>(params.mean, params.sd);
    uniform_dist = new std::uniform_real_distribution<double>(0, 1);

    auto *seed_dist = new std::uniform_int_distribution<int>(0, INT_MAX);
    for (int i = 0; i < 4; ++i) {
        generators[i].seed((long) (*seed_dist)(generator));
    }
}


/**
 * Indiscriminately cancel order with a probability of p_cancel at each step in an LOB.
 * @param lob
 * @param orders
 * @param p_cancel
 * @param l1
 */
void ZiBaseTrader::cancel_orders(deque<lob_order> *orders, L1 * l1) {

    unsigned long int step = l1->step;
    const string &time = l1->time;

    for (const auto &o : queued_orders) {

        double p = (*uniform_dist)(generators[DELTA]);
        double p_cancel = get_delta();
        if (p < p_cancel) {
            lob_order _ord = get_cancel_order(o, step, time);
            orders->push_back(_ord);
        }
    }
}


/**
 * Zero-intelligence limit orders.
 * @param orders
 * @param l1
 */
void ZiBaseTrader::submit_limit_orders(deque<lob_order> *orders, L1 * l1) {

    unsigned long int step = l1->step;
    const string &time = l1->time;

    double alpha = get_alpha();
    double p = (*uniform_dist)(generators[ALPHA]);
    if (p < alpha) {
        Side side = get_side();
        unsigned int vol = get_limit_vol();
        unsigned int prc = get_prc(side, l1);
        string order_id = agentID + "-l-" + get_traderType() + "-" + std::to_string(sequence++);
        lob_order _ord = get_limit_order(prc, vol, order_id, step, time, side, get_message());

        orders->push_back(_ord);
    }
}


/**
 * Zero-intelligence market orders.
 * @param orders
 * @param l1
 */
void ZiBaseTrader::submit_market_orders(deque<lob_order> *orders, L1 * l1) {

    unsigned long int step = l1->step;
    const string &time = l1->time;

    double mu = get_mu();
    double p = (*uniform_dist)(generators[MU]);
    if (p < mu) {
        Side side = get_side();
        unsigned int vol = get_market_vol();
        string order_id = agentID + "-m-" + std::to_string(sequence++);
        lob_order _ord = get_market_order(vol, order_id, step, time, side, get_message());

        orders->push_back(_ord);
    }
}


/**
 * For Zero-Intelligence strategy, a limit order is placed at a distance dp from the mid-price,
 * where dp follows an exponential distribution, E[lambda].
 * @param side
 * @param l1
 * @return a price in tick
 */
unsigned int ZiBaseTrader::get_prc(Side side, L1 * l1) {

    double dp = (*dp_dist)(generators[DP]);
    double mid_prc = l1->get_mid_prc();

    return side == Side::BUY ? (int)round(mid_prc - dp) : (int)round(mid_prc + dp);
}


/**
 *
 * @param orders
 * @param l1
 */
void ZiBaseTrader::get_orders(deque<lob_order> *orders, L1 * l1) {
    cancel_orders(orders, l1);
    submit_limit_orders(orders, l1);
    submit_market_orders(orders, l1);
}

double ZiBaseTrader::get_delta() const {
    return ziBaseParams.delta;
}


void gatewayAgentBase::handle_new_report(execution_report *report) {
    // Remove an existing order (i.e., a partial fill) if an order is partially filled when placed.
    queued_orders.erase(std::remove_if(queued_orders.begin(), queued_orders.end(),
                                       [&](const lob_order& o) { return report->order_id == o.order_id; }),
                        queued_orders.end());

    assert(report->orderType != OrdType::MARKET);

    // Add a new order.
    lob_order o1 = {
            .prc = report->prc,
            .vol = report->vol,
            .order_id = report->order_id,
            .t = report->t,
            .side = report->side,
            .orderType = report->orderType
    };

    queued_orders.push_back(o1);
}

void gatewayAgentBase::handle_amend_report(execution_report *report) {

    // Add a amend order.
    lob_order amend_order = {
            .prc = report->prc,
            .vol = report->vol,
            .order_id = report->order_id,
            .t = report->t,
            .side = report->side,
            .orderType = report->orderType
    };

    queued_orders.erase(std::remove_if(queued_orders.begin(), queued_orders.end(),
                                       [&](const lob_order& o) { return amend_order.order_id == o.order_id; }),
                        queued_orders.end());
    queued_orders.push_back(amend_order);
}

void gatewayAgentBase::handle_cancel_report(execution_report *report) {

    // Cancel an existing order.
    queued_orders.erase(std::remove_if(queued_orders.begin(), queued_orders.end(),
                                       [&](const lob_order& o) { return report->order_id == o.order_id; }),
                        queued_orders.end());
}

void gatewayAgentBase::handle_fill_report(execution_report *report) {
    update_position(report);

    // Remove the filled order.
    queued_orders.erase(std::remove_if(queued_orders.begin(), queued_orders.end(),
                                       [&](const lob_order& o) { return report->order_id == o.order_id; }),
                        queued_orders.end());
}

void gatewayAgentBase::handle_partial_fill_report(execution_report *report) {
    update_position(report);

    if(report->orderType == OrdType::MARKET)
        return;

    // Remove the filled order and add the new order.
    lob_order order = {
            .prc = report->prc,
            .vol = report->vol,
            .order_id = report->order_id,
            .t = report->t,
            .side = report->side,
            .orderType = report->orderType
    };

    queued_orders.erase(std::remove_if(queued_orders.begin(), queued_orders.end(),
                                       [&](const lob_order& o) { return report->order_id == o.order_id; }),
                        queued_orders.end());
    queued_orders.push_back(order);
}

void gatewayAgentBase::handle_rejected_report(execution_report *report) {
    // Do nothing. An order can be rejected if a cancel is sent and before the cancel is executed, the order is filled.
}