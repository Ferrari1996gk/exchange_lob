//
// Created by kgao_smd on 19/01/2022.
//
#include "marketMaker.h"

marketMaker::marketMaker(long seed, const string &agentID, const mm_params &mmParams, float tick_size) :
            gatewayAgentBase::gatewayAgentBase(seed, agentID){
    marketMaker::mmParams = mmParams;
    marketMaker::tick_size = tick_size;
    uniform_dist = new std::uniform_real_distribution<double>(0, 1);

    auto *seed_dist = new std::uniform_int_distribution<int>(0, INT_MAX);
    for (int i = 0; i < 4; ++i) {
        generators[i].seed((long) (*seed_dist)(generator));
    }
}

void marketMaker::handle_l1_report(const L1 *l1) {
    gatewayAgentBase::handle_l1_report(l1);
}

void marketMaker::cancel_orders(deque<lob_order> *orders, L1 * l1) {

    unsigned long int step = l1->step;
    const string &time = l1->time;

    for (const auto &o : queued_orders) {

        double p = (*uniform_dist)(generators[CANCEL]);
        double p_cancel = get_delta();
        if (p < p_cancel) {
            lob_order _ord = get_cancel_order(o, step, time);
            orders->push_back(_ord);
        }
    }
}

void marketMaker::cancel_all_orders(deque<lob_order> *orders, L1 * l1) {

    unsigned long int step = l1->step;
    const string &time = l1->time;
    for (const auto &o : queued_orders) {
        lob_order _ord = get_cancel_order(o, step, time);
        orders->push_back(_ord);
    }
}

void marketMaker::submit_limit_orders(deque<lob_order> *orders, L1 * l1) {

    unsigned long int step = l1->step;
    const string &time = l1->time;

    double alpha = get_alpha();
    double p = (*uniform_dist)(generators[LIMIT]);
    if (p < alpha) {
        unsigned int vol = get_limit_vol();
        if (position < mmParams.mm_pos_limit){
            unsigned int bid_prc = get_bid_prc(l1);
            string bid_order_id = agentID + "-l-" + get_traderType() + "-" + std::to_string(sequence++);
            lob_order _bid_ord = get_limit_order(bid_prc, vol, bid_order_id, step, time, Side::BUY, get_message());
            orders->push_back(_bid_ord);
        }

        if (position > -1 * mmParams.mm_pos_limit) {
            unsigned int ask_prc = get_ask_prc(l1);
            string ask_order_id = agentID + "-l-" + get_traderType() + "-" + std::to_string(sequence++);
            lob_order _ask_ord = get_limit_order(ask_prc, vol, ask_order_id, step, time, Side::SELL, get_message());
            orders->push_back(_ask_ord);
        }

    }
}

void marketMaker::submit_market_orders(deque<lob_order> *orders, L1 *l1) {
    unsigned long int step = l1->step;
    const string &time = l1->time;

    Side side = position > 0? Side::SELL : Side::BUY;
    unsigned int vol = get_market_vol();
    string order_id = agentID + "-m-" + std::to_string(sequence++);
    lob_order _ord = get_market_order(vol, order_id, step, time, side, get_message());

    orders->push_back(_ord);
}

unsigned int marketMaker::get_bid_prc(L1 * l1) {

    double dp = (*uniform_dist)(generators[BID_DP]);
    double mid_prc = l1->get_mid_prc();

    return (int)round(mid_prc - dp * mmParams.mm_edge);
}

unsigned int marketMaker::get_ask_prc(L1 * l1) {

    double dp = (*uniform_dist)(generators[ASK_DP]);
    double mid_prc = l1->get_mid_prc();

    return (int)round(mid_prc + dp * mmParams.mm_edge);
}

//unsigned int marketMaker::get_bid_prc1(L1 * l1) {
//
//    double dp = (*uniform_dist)(generators[BID_DP]);
//    unsigned int best_bid_prc = l1->get_best_bid_prc();
//
//    return best_bid_prc - (int)round(dp * mmParams.mm_edge);
//}
//
//unsigned int marketMaker::get_ask_prc1(L1 * l1) {
//
//    double dp = (*uniform_dist)(generators[ASK_DP]);
//    unsigned int best_ask_prc = l1->get_best_ask_prc();
//
//    return best_ask_prc + (int)round(dp * mmParams.mm_edge);
//}


void marketMaker::get_orders(deque<lob_order> *orders, L1 * l1) {
    if (position >= mmParams.mm_pos_limit || position <= -1 * mmParams.mm_pos_limit){
        cout << "MM pos limit hit! " << l1->time << endl;
        mo_flag = true;
        start_step = l1->step + mmParams.mm_rest;
    }
    if (mo_flag && position < mmParams.mm_pos_safe && position > -1 * mmParams.mm_pos_safe){
        cout << "Back to safe position" << endl;
        mo_flag = false;
    }
    if (mo_flag){
        cancel_all_orders(orders, l1);
        submit_market_orders(orders, l1);
    } else if (l1->step > start_step){
        cancel_orders(orders, l1);
        submit_limit_orders(orders, l1);
    }
}

