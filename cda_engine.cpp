//
// Created by Perukrishnen Vytelingum on 06/04/2020.
//

#include <iomanip>
#include <fstream>
#include <iostream>
#include <algorithm>

#include <boost/filesystem.hpp>

#include "cda_engine.h"

using namespace std;

const int MIN_PRC = 1;
const int MAX_PRC = 1000000;

Engine::Engine(const string& symbol, const int * reference_price, float tick_size, const string& output_path,
        int verbose, const string& tick_format) {
    Engine::tick_size = tick_size;
    Engine::verbose = verbose;
    Engine::symbol = symbol;
    Engine::tick_format = get_tick_format(tick_format);

    Engine::reference_price[0] = reference_price[0];
    Engine::reference_price[1] = reference_price[1];

    Engine::min_prc = reference_price[0];
    Engine::max_prc = reference_price[1];

    Engine::sequence = 0;
    Engine::numTransactions = 0;

    if(!output_path.empty()) {

        boost::filesystem::path dir(output_path);
        if(boost::filesystem::create_directory(dir)) {
            //std::cerr << "Directory Created: " << output_path << std::endl;
        }

        string filepath = output_path + "/";

        if(verbose >= 3) {
            if(Engine::tick_format == TickFormat::SGX) {
                fs_tick_orders.open(filepath + "TickData_" + symbol + ".csv", std::fstream::out);
                fs_tick_orders << "sym,timestamp,sequence_number,ob_position,quantity_difference,ob_command,change_reason,";
                fs_tick_orders << "order_number,price,mp_quantity,bid_or_ask,agent_id,TraderType" << endl;
            }
            else if(Engine::tick_format == TickFormat::LSE) {
                fs_tick_orders.open(filepath + "LitOrders.csv", std::fstream::out);
                fs_tick_orders << ",PARTITION_ID,DSS_SEQUENCE,TRANSACTTIME,MITSEQUENCE,PUBLIC_ORDER_ID,SYMBOL,ISIN,";
                fs_tick_orders << "CURRENCY,MIC,EXECTYPE,ORDERBOOK,SIDE,VENUE,PRICE,VISIBLEQTY,ORDERTYPE,TRADEREPORTID,";
                fs_tick_orders << "EXECUTED_QUANTITY" << endl;

                fs_tick_trades.open(filepath + "LitTrades.csv", std::fstream::out);
                fs_tick_trades << ",TOPICPARTITIONID,DSSSEQUENCENUMBER,TRANSACTTIME,MITSEQUENCENUMBER,SYMBOL,ISIN,CURRENCY,";
                fs_tick_trades << "MIC,EXECTYPE,ORDERBOOK,SIDE,VENUE,EXECUTEDPRICE,EXECUTEDSIZE,TRADEREPORTID,";
                fs_tick_trades << "PUBLICORDERID,TRADESESSION" << endl;
            }
        }

        if(verbose >= 0) {
            fs_lob_l1.open(filepath + "lob_l1.csv", std::fstream::out);
            fs_lob_l1 << "sym,step,prc,vol,depth,total_vol,side,time,num" << endl;
        }

        if(verbose >= 1) {
            fs_trades.open(filepath + "trades.csv", std::fstream::out);
            fs_trades << "sym,prc,vol,bid_id,ask_id,buyer_id,seller_id,step,time" << endl;
        }

        if(verbose >= 2) {
            fs_orders.open(filepath + "orders.csv", std::fstream::out);
            fs_orders << "sym,prc,vol,order_id,agent_id,t,expiry,side,OrdType,time,TraderType,message" << endl;
        }

        if(verbose >= 4) {
            fs_lob_l2.open(filepath + "lob_l2.csv", std::fstream::out);
            fs_lob_l2 << "sym,step,level,prc,vol,num,side,time" << endl;
        }

        if(verbose >= 5) {
            fs_lob.open(filepath + "lob.csv", std::fstream::out);
            fs_lob << "sym,step,level,prc,vol,order_id,agent_id,t,expiry,side,time" << endl;

            fs_orders_cancelled.open(filepath + "orders_cancelled.csv", std::fstream::out);
            fs_orders_cancelled << "sym,prc,vol,order_id,agent_id,t,expiry,side,OrdType,time,TraderType" << endl;
        }
    }
}

unsigned int * Engine::get_reference_prc() {
    return reference_price;
}

/**
 * Add a new order to the limit order book.
 * TODO: check a market order never stays on the LOB.
 * @param order
 */
void Engine::process_order(lob_order & o, deque<execution_report> * reports, deque<transaction> * transactions) {

    // Write order to file.
    if (verbose >= 1) {
        if (fs_orders.is_open()) {
            fs_orders << symbol << "," << (double) o.prc * tick_size << ',' << o.vol << ',' << o.order_id << ',' << o.agent_id << ','
                      << std::setprecision(12) << o.t << std::setprecision(6) << ',';
            fs_orders << o.expiry << ',' << (char) o.side << ',' << (char) o.orderType << ',' << o.time << ',';
            fs_orders << o.traderType << ',' << o.message << endl;
        }
    }

    if (o.orderType == OrdType::LIMIT) {
        clear_order(o, reports, transactions, false);
    } else if (o.orderType == OrdType::AMEND) {
        clear_order(o, reports, transactions, true);
    } else if (o.orderType == OrdType::MARKET) {
        o.expiry = o.t;
        if (o.side == Side::BUY)
            o.prc = MAX_PRC;
        else
            o.prc = MIN_PRC;

        clear_order(o, reports, transactions, false);
    } else if (o.orderType == OrdType::CANCEL) {
        bool is_rejected = false;
        cancel_order(o, reports, false, &is_rejected);

        if (verbose >= 3 && fs_orders_cancelled.is_open()) {
            fs_orders_cancelled << symbol << "," << (float) o.prc * tick_size << ',' << o.vol << ',' << o.order_id
                                << ',' << o.agent_id << ',' << std::setprecision(12) << o.t << std::setprecision(6)
                                << ',' << o.expiry << ',' << (char) o.side << ',' << (char) o.orderType << ',' << o.time
                                << ',' << o.traderType << endl;
        }
    }

    update_reference_prc();

    // Output tick data.
    if (verbose >= -1) {
        if (tick_format == TickFormat::SGX) {
            if (fs_tick_orders.is_open()) {

                for (const auto &report : *reports) {

                    auto ob_position = get_ob_position(report.order_id, report.side);

                    if (report.executionReportType == ExecutionReportType::NEW) {
                        //ob_command: Add|0  change_reason: Added|6
                        fs_tick_orders << symbol << "," << o.time << "," << sequence << "," << ob_position << ","
                                       << o.vol << "," << 0 << "," << 6 << "," << o.order_id << ","
                                       << (unsigned long) ((float) o.prc * tick_size);

                    } else if (report.executionReportType == ExecutionReportType::CANCEL ||
                               report.executionReportType == ExecutionReportType::EXPIRED) {
                        //ob_command: Delete|1  change_reason: Deleted|1
                        fs_tick_orders << symbol << "," << o.time << "," << sequence << "," << ob_position << ","
                                       << -(int) o.vol << "," << 1 << "," << 1 << "," << o.order_id << ","
                                       << (unsigned long) ((float) o.prc * tick_size);

                    } else if (report.executionReportType == ExecutionReportType::FILL) {
                        //ob_command: Delete|1  change_reason: Traded|3
                        fs_tick_orders << symbol << "," << o.time << "," << sequence << "," << ob_position << ","
                                       << -(int) report.executedQuantity << "," << 1 << "," << 3 << "," << o.order_id
                                       << "," << (unsigned long) ((float) report.executedPrice * tick_size);

                    } else if (report.executionReportType == ExecutionReportType::PARTIAL_FILL) {
                        //ob_command: Change|2  change_reason: Traded|3
                        fs_tick_orders << symbol << "," << o.time << "," << sequence << "," << ob_position << ","
                                       << -(int) report.executedQuantity << "," << 2 << "," << 3 << "," << o.order_id
                                       << "," << (unsigned long) ((float) report.executedPrice * tick_size);

                    } else if (report.executionReportType == ExecutionReportType::AMEND) {
                        //ob_command: Change|2  change_reason: Amended|5
                        fs_tick_orders << symbol << "," << o.time << "," << sequence << "," << ob_position << ","
                                       << report.amendedQuantity << "," << 2 << "," << 5 << "," << o.order_id << ","
                                       << (unsigned long) ((float) o.prc * tick_size);

                    } else {
                        // Order rejected, e.g., being filled before being cancelled.
                        continue;
                    }

                    fs_tick_orders << "," << o.vol << ",";
                    fs_tick_orders << (o.side == Side::BUY ? 1 : 2) << "," << o.agent_id << "," << o.traderType << endl;
                }
            }
        }
        else if(tick_format == TickFormat::LSE) {

            if (fs_tick_orders.is_open() && fs_tick_trades.is_open()) {

                for (const auto &report : *reports) {

                    string side = report.side == Side::BUY ? "Buy" : "Sell";

                    if(report.executionReportType == ExecutionReportType::NEW) {
                        fs_tick_orders << ",," << sequence << "," << report.time << ",," << report.order_id << ","
                                       << symbol << ",," << ",,Insert,," << side << ",TGHL,"
                                       << (float) report.prc * tick_size << "," << report.vol << ",Limit,," << endl;
                    }
                    else if(report.executionReportType == ExecutionReportType::CANCEL) {
                        fs_tick_orders << ",," << sequence << "," << report.time << ",," << report.order_id << ","
                                       << symbol << ",," << ",,Cancel,," << side << ",TGHL,"
                                       << (float) report.prc * tick_size << "," << 0 << ",Limit,," << endl;
                    }
                    else if(report.executionReportType == ExecutionReportType::PARTIAL_FILL) {
                        fs_tick_orders << ",," << sequence << "," << report.time << ",," << report.order_id << ","
                                       << symbol << ",," << ",,Fill,," << side << ",TGHL,"
                                       << (float) report.executedPrice * tick_size << "," << report.vol << ",Limit,,"
                                       << report.vol << endl;

                        fs_tick_trades << ",," << sequence << "," << report.time << ",," << symbol << ",,," << ","
                                       << "Fill,,Buy,TGHL," << (float) report.executedPrice * tick_size
                                       << "," << report.executedQuantity << ",," << report.order_id << "," << endl;

                        fs_tick_trades << ",," << sequence << "," << report.time << ",," << symbol << ",,," << ","
                                       << "Fill,,Sell,TGHL," << (float) report.executedPrice * tick_size
                                       << "," << report.executedQuantity << ",," << report.order_id << "," << endl;
                    }
                    else if(report.executionReportType == ExecutionReportType::FILL) {
                        assert(report.vol == 0);
                        fs_tick_orders << ",," << sequence << "," << report.time << ",," << report.order_id << ","
                                       << symbol << ",," << ",,Fill,," << side << ",TGHL,"
                                       << (float) report.executedPrice * tick_size << "," << report.vol << ",Limit,,"
                                       << report.executedQuantity << endl;

                        fs_tick_trades << ",," << sequence << "," << report.time << ",," << symbol << ",,," << ","
                                       << "Fill,,Buy,TGHL," << (float) report.executedPrice * tick_size
                                       << "," << 0 << ",," << report.order_id << "," << endl;

                        fs_tick_trades << ",," << sequence << "," << report.time << ",," << symbol << ",,," << ","
                                       << "Fill,,Sell,TGHL," << (float) report.executedPrice * tick_size
                                       << "," << 0 << ",," << report.order_id << "," << endl;
                    }
                    else if(report.executionReportType == ExecutionReportType::AMEND) {
                        fs_tick_orders << ",," << sequence << "," << report.time << ",," << report.order_id << ","
                                       << symbol << ",,";
                        fs_tick_orders << ",,Amend,," << side << ",TGHL," << (float) report.prc * tick_size << ","
                                       << report.vol << ",Limit,," << endl;
                    }
                    else if(report.executionReportType == ExecutionReportType::REJECTED) {
                        // Do nothing
                    }
                    else if(report.executionReportType == ExecutionReportType::EXPIRED) {
                        // Do nothing
                    }
                }
            }
        }
    }

    sequence++;
}


/**
 * Update reference price. When LOB is empty, reference price is set to 2% away from high/low.
 */
void Engine::update_reference_prc() {

    // Minimum and maximum prices on the LOB.
    if(!buy_lob.empty() && !sell_lob.empty()) {
        reference_price[0] = buy_lob.front().prc;
        reference_price[1] = sell_lob.front().prc;

        min_prc = min(min_prc, reference_price[0]);
        max_prc = max(max_prc, reference_price[1]);
    }
    else if (!buy_lob.empty() && sell_lob.empty()) {
        reference_price[0] = buy_lob.front().prc;
        reference_price[1] = reference_price[1] == 0 ? max(reference_price[0] + 1, (unsigned int) (max_prc * 1.02))
                                                     : reference_price[1];

        min_prc = min(min_prc, reference_price[0]);
    }
    else if (buy_lob.empty() && !sell_lob.empty()) {
        reference_price[1] = sell_lob.front().prc;
        reference_price[0] = reference_price[0] == 0 ? min((unsigned int) (min_prc * 0.98), reference_price[1] - 1)
                                                     : reference_price[0];
        max_prc = max(max_prc, reference_price[1]);
    }
    else if (buy_lob.empty() && sell_lob.empty()) {
        reference_price[0] = reference_price[0] == 0 ? (unsigned int) (min_prc * 0.98) : reference_price[0];
        reference_price[1] = reference_price[1] == 0 ? max(reference_price[0] + 1, (unsigned int) (max_prc * 1.02))
                                                     : reference_price[1];
    }

    // TODO: add when we specify an auction phase.
    // assert(reference_price[1] > reference_price[0]);
}


/**
 * Add a buy or a sell limit order.
 * @param o
 */
void Engine::add_order(lob_order &o) {
    if(o.side == Side::SELL) {
        sell_lob.push_front(o);
        sort(sell_lob.begin(), sell_lob.end());
    }
    else if (o.side == Side::BUY) {
        buy_lob.push_front(o);
        sort(buy_lob.begin(), buy_lob.end());
    }
}


/**
 * Cancel an order given the order_id.
 * TODO: optimise.
 * @param cancel_order
 */
void Engine::cancel_order(lob_order &cancel_order, deque<execution_report> * reports, bool is_amend, bool * is_rejected) {
    if(cancel_order.side == Side::SELL) {
            if (std::any_of(sell_lob.begin(), sell_lob.end(), [&](const lob_order &o) {
                return cancel_order.order_id == o.order_id;
            })) {

                if (!is_amend) {
                    execution_report _report = {
                            .executionReportType=ExecutionReportType::CANCEL,
                            .t = cancel_order.t,
                            .time = cancel_order.time,
                            .agent_id=cancel_order.agent_id,
                            .side=Side::SELL,
                            .prc = cancel_order.prc,
                            .vol = cancel_order.vol,
                            .order_id = cancel_order.order_id,
                            .orderType = cancel_order.orderType,
                            .traderType = cancel_order.traderType,
                            .agent_index = cancel_order.agent_index
                    };
                    reports->push_back(_report);
                }
            }
            else {
                execution_report _report = {
                        .executionReportType=ExecutionReportType::REJECTED,
                        .reason="Sell Order does not exist on LOB",
                        .t = cancel_order.t,
                        .time = cancel_order.time,
                        .agent_id=cancel_order.agent_id,
                        .side=Side::SELL,
                        .prc = cancel_order.prc,
                        .vol = cancel_order.vol,
                        .order_id = cancel_order.order_id,
                        .orderType = cancel_order.orderType,
                        .traderType = cancel_order.traderType,
                        .agent_index = cancel_order.agent_index
                };
                reports->push_back(_report);

                (*is_rejected) = true;
                return;
            }

        sell_lob.erase(std::remove_if(sell_lob.begin(), sell_lob.end(),
                [&](const lob_order& o) { return cancel_order.order_id == o.order_id; }), sell_lob.end());
    }
    else {
        if (std::any_of(buy_lob.begin(), buy_lob.end(), [&](const lob_order &o) {
            return cancel_order.order_id == o.order_id;
        })) {
            if (!is_amend) {
                execution_report _report = {
                        .executionReportType=ExecutionReportType::CANCEL,
                        .t = cancel_order.t,
                        .time = cancel_order.time,
                        .agent_id=cancel_order.agent_id,
                        .side=Side::BUY,
                        .prc = cancel_order.prc,
                        .vol = cancel_order.vol,
                        .order_id = cancel_order.order_id,
                        .orderType = cancel_order.orderType,
                        .traderType = cancel_order.traderType,
                        .agent_index = cancel_order.agent_index
                };
                reports->push_back(_report);
            }
        }
        else {
            execution_report _report = {
                    .executionReportType=ExecutionReportType::REJECTED,
                    .reason="Buy Order does not exist on LOB",
                    .t = cancel_order.t,
                    .time = cancel_order.time,
                    .agent_id=cancel_order.agent_id,
                    .side=Side::BUY,
                    .prc = cancel_order.prc,
                    .vol = cancel_order.vol,
                    .order_id = cancel_order.order_id,
                    .orderType = cancel_order.orderType,
                    .traderType = cancel_order.traderType,
                    .agent_index = cancel_order.agent_index
            };
            reports->push_back(_report);

            (*is_rejected) = true;
            return;
        }

        buy_lob.erase(std::remove_if(buy_lob.begin(), buy_lob.end(),
                                     [&](const lob_order &o) { return cancel_order.order_id == o.order_id; }),
                      buy_lob.end());
    }

    (*is_rejected) = false;
    update_reference_prc();
}


/**
 * Clear a limit order.
 * @param o
 */
void Engine::clear_order(lob_order &o, deque<execution_report> * reports, deque<transaction> * transactions, bool is_amend) {

    bool is_rejected = false;

    if(is_amend)
        cancel_order(o, reports, true, &is_rejected);

    if(is_rejected)
        return;

    if(o.side == Side::BUY)
        clear_bid(o, reports, transactions, is_amend);
    else if(o.side == Side::SELL)
        clear_ask(o, reports, transactions, is_amend);
}


/**
 * Clear an ask against the buy limit order book.
 * @param ask
 */
void Engine::clear_ask(lob_order &ask, deque<execution_report> * reports, deque<transaction> * transactions, bool is_amend) {
    if(ask.side == Side::BUY) {
        cout << "ERROR 1" << endl;
        throw simuException();
    }

    for(auto bid = buy_lob.begin(); bid != buy_lob.end();) {

        if((*bid).prc >= ask.prc) {
            unsigned int vol = min(ask.vol, (*bid).vol);

            transaction tr = {.prc=(*bid).prc, .vol=vol, .bid_id=(*bid).order_id,
                              .ask_id=ask.order_id, .buyer_id=(*bid).agent_id, .seller_id=ask.agent_id, .t=ask.t,
                              .time=ask.time};
            transactions->push_back(tr);
            numTransactions++;

            if(verbose >= 0 && fs_trades.is_open()) {
                fs_trades << symbol << "," << (float) tr.prc * tick_size << ',' << tr.vol << ',' << tr.bid_id << ','
                          << tr.ask_id << ',' << tr.buyer_id << ',' << tr.seller_id << ','
                          << std::setprecision(12) << tr.t << std::setprecision(6) << ',' << tr.time << endl;
            }

            (*bid).vol -= vol;
            ask.vol -= vol;

            if ((*bid).vol == 0) {

                execution_report _report = {
                        .executionReportType=ExecutionReportType::FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=bid->agent_id,
                        .side=Side::BUY,
                        .prc=bid->prc,
                        .vol=0,
                        .order_id = bid->order_id,
                        .orderType = bid->orderType,
                        .traderType = bid->traderType,
                        .agent_index = bid->agent_index
                };
                reports->push_back(_report);
                bid = buy_lob.erase(bid);
            }
            else {
                assert(bid->vol > 0);
                execution_report _report = {
                        .executionReportType=ExecutionReportType::PARTIAL_FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=bid->agent_id,
                        .side=Side::BUY,
                        .prc=bid->prc,
                        .vol=bid->vol,
                        .order_id = bid->order_id,
                        .orderType = bid->orderType,
                        .traderType = bid->traderType,
                        .agent_index = bid->agent_index
                };
                reports->push_back(_report);
                bid++;
            }

            if (ask.vol == 0) {
                execution_report _report = {
                        .executionReportType=ExecutionReportType::FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=ask.agent_id,
                        .side=Side::SELL,
                        .prc=ask.prc,
                        .vol=0,
                        .order_id = ask.order_id,
                        .orderType = ask.orderType,
                        .traderType = ask.traderType,
                        .agent_index = ask.agent_index
                };
                reports->push_back(_report);

                break;
            }
            else {
                assert(ask.vol > 0);

                execution_report _report = {
                        .executionReportType=ExecutionReportType::PARTIAL_FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=ask.agent_id,
                        .side=Side::SELL,
                        .prc=ask.prc,
                        .vol=ask.vol,
                        .order_id = ask.order_id,
                        .orderType = ask.orderType,
                        .traderType = ask.traderType,
                        .agent_index = ask.agent_index
                };
                reports->push_back(_report);
            }
        }
        else {
            break;
        }
    }

    if(ask.vol > 0) {
        if(ask.orderType == OrdType::LIMIT || ask.orderType == OrdType::AMEND) {

            unsigned int _vol = ask.orderType == OrdType::AMEND ? get_order_vol(ask.order_id, Side::SELL) : 0;
            add_order(ask);
            execution_report _report = {
                    .executionReportType=is_amend ? ExecutionReportType::AMEND : ExecutionReportType::NEW,
                    .amendedQuantity = ask.vol - _vol,
                    .t=ask.t,
                    .time=ask.time,
                    .agent_id=ask.agent_id,
                    .side=Side::SELL,
                    .prc=ask.prc,
                    .vol=ask.vol,
                    .order_id = ask.order_id,
                    .orderType = ask.orderType,
                    .traderType = ask.traderType,
                    .agent_index = ask.agent_index
            };
            reports->push_back(_report);
        }
    }
}


/**
 * Clear a bid against a sell order book.
 * @param bid
 */
void Engine::clear_bid(lob_order& bid, deque<execution_report> * reports, deque<transaction> * transactions, bool is_amend) {
    if(bid.side == Side::SELL) {
        cout << "ERROR 2" << endl;
        throw simuException();
    }

    for(auto ask = sell_lob.begin(); ask != sell_lob.end();) {

        if(bid.prc >= (*ask).prc) {
            unsigned int vol = min((*ask).vol, bid.vol);

            transaction tr = { .prc=(*ask).prc, .vol=vol, .bid_id=bid.order_id, .ask_id=(*ask).order_id,
                               .buyer_id=bid.agent_id, .seller_id=(*ask).agent_id, .t=bid.t, .time=bid.time };
            transactions->push_back(tr);
            numTransactions++;

            if(verbose >= 0 && fs_trades.is_open()) {
                fs_trades << symbol << "," << (float)tr.prc * tick_size << ',' << tr.vol << ',';
                fs_trades << tr.bid_id << ',' << tr.ask_id << ',' << tr.buyer_id << ',' << tr.seller_id << ','
                          << std::setprecision(12) << tr.t << std::setprecision(6) << ',' << tr.time << endl;
            }

            bid.vol -= vol;
            (*ask).vol -= vol;

            if((*ask).vol == 0) {
                execution_report _report = {
                        .executionReportType=ExecutionReportType::FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=ask->agent_id,
                        .side=Side::SELL,
                        .prc=ask->prc,
                        .vol=0,
                        .order_id = ask->order_id,
                        .orderType = ask->orderType,
                        .traderType = ask->traderType,
                        .agent_index = ask->agent_index
                };
                reports->push_back(_report);

                ask = sell_lob.erase(ask);
            }
            else {
                assert(ask->vol > 0);
                execution_report _report = {
                        .executionReportType=ExecutionReportType::PARTIAL_FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=ask->agent_id,
                        .side=Side::SELL,
                        .prc=ask->prc,
                        .vol=ask->vol,
                        .order_id = ask->order_id,
                        .orderType = ask->orderType,
                        .traderType = ask->traderType,
                        .agent_index = ask->agent_index
                };
                reports->push_back(_report);
                ask++;
            }

            if(bid.vol == 0) {
                execution_report _report = {
                        .executionReportType=ExecutionReportType::FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=bid.agent_id,
                        .side=Side::BUY,
                        .prc=bid.prc,
                        .vol=0,
                        .order_id = bid.order_id,
                        .orderType = bid.orderType,
                        .traderType = bid.traderType,
                        .agent_index = bid.agent_index
                };
                reports->push_back(_report);

                break;
            }
            else {
                assert(bid.vol > 0);
                execution_report _report = {
                        .executionReportType=ExecutionReportType::PARTIAL_FILL,
                        .executedQuantity=tr.vol,
                        .executedPrice=tr.prc,
                        .t=tr.t,
                        .time=tr.time,
                        .agent_id=bid.agent_id,
                        .side=Side::BUY,
                        .prc=bid.prc,
                        .vol=bid.vol,
                        .order_id = bid.order_id,
                        .orderType = bid.orderType,
                        .traderType = bid.traderType,
                        .agent_index = bid.agent_index
                };
                reports->push_back(_report);
            }
        }
        else {
            break;
        }
    }

    if(bid.vol > 0) {
        if(bid.orderType == OrdType::LIMIT || bid.orderType == OrdType::AMEND) {

            unsigned int _vol = bid.orderType == OrdType::AMEND ? get_order_vol(bid.order_id, Side::BUY) : 0;
            add_order(bid);
            execution_report _report = {
                    .executionReportType=is_amend ? ExecutionReportType::AMEND : ExecutionReportType::NEW,
                    .amendedQuantity = bid.vol - _vol,
                    .t=bid.t,
                    .time=bid.time,
                    .agent_id=bid.agent_id,
                    .side=Side::BUY,
                    .prc=bid.prc,
                    .vol=bid.vol,
                    .order_id = bid.order_id,
                    .orderType = bid.orderType,
                    .traderType = bid.traderType,
                    .agent_index = bid.agent_index
            };
            reports->push_back(_report);
        }
    }
}


/**
 * Remove expired orders.
 * TODO: optimise code here.
 */
bool Engine::expire(double t, deque<execution_report> * reports) {

    bool has_expired = false;
    for(auto& o : buy_lob) {
        if(o.expiry <= t) {
            has_expired = true;

            // TODO: publish expired reports.
            execution_report _report = {
                    .executionReportType=ExecutionReportType::EXPIRED,
                    .t=o.t,
                    .time=o.time,
                    .agent_id=o.agent_id,
                    .side=Side::BUY,
                    .prc=o.prc,
                    .vol=o.vol,
                    .order_id = o.order_id,
                    .orderType = o.orderType,
                    .traderType = o.traderType,
                    .agent_index = o.agent_index
            };
            reports->push_back(_report);

            if(verbose >= 3 && fs_orders_cancelled.is_open()) {
                fs_orders_cancelled << symbol << "," << (float) o.prc * tick_size << ',' << o.vol << ',' << o.order_id << ','
                                    << o.agent_id << ',' << std::setprecision(12) << o.t << std::setprecision(6) << ',';
                fs_orders_cancelled << o.expiry << ',' << (char) o.side << ',' << 'E' << ',' << o.time << endl;
            }
            else {
                break;
            }
        }
    }

    for(auto& o : sell_lob) {
        if(o.expiry <= t) {
            has_expired = true;

            execution_report _report = {
                    .executionReportType=ExecutionReportType::EXPIRED,
                    .t=o.t,
                    .time=o.time,
                    .agent_id=o.agent_id,
                    .side=Side::SELL,
                    .prc=o.prc,
                    .vol=o.vol,
                    .order_id = o.order_id,
                    .orderType = o.orderType,
                    .traderType = o.traderType,
                    .agent_index = o.agent_index
            };
            reports->push_back(_report);

            if(verbose >= 3 && fs_orders_cancelled.is_open()) {
                fs_orders_cancelled << symbol << "," << (float) o.prc * tick_size << ',' << o.vol << ',' << o.order_id
                                    << ',' << o.agent_id << ',' << std::setprecision(12) << o.t << std::setprecision(6)
                                    << ',' << o.expiry << ',' << (char) o.side << ',' << 'E' << ',' << o.time << endl;
            }
            else {
                break;
            }
        }
    }

    buy_lob.erase(std::remove_if(buy_lob.begin(), buy_lob.end(),
            [&](const lob_order& o) { return t >= o.expiry; }), buy_lob.end());

    sell_lob.erase(std::remove_if(sell_lob.begin(), sell_lob.end(),
            [&](const lob_order& o) { return t >= o.expiry; }), sell_lob.end());

    update_reference_prc();

    return has_expired;
}


void Engine::save_l3(double t, const string& time) {

    if(!buy_lob.empty()) {
        int level = 0;
        for (const auto& o : buy_lob) {
            fs_lob << symbol << "," << std::setprecision(12) << t << std::setprecision(6) << ',' << level++ << ','
                   << (float) o.prc * tick_size << "," << o.vol << "," << o.order_id << "," << o.agent_id << ","
                   << std::setprecision(12) << o.t << std::setprecision(6) << "," << o.expiry << "," << (char) o.side
                   << ',' << time << endl;
        }
    }

    if(!sell_lob.empty()) {
        int level = 0;
        for (const auto& o : sell_lob) {
            fs_lob << symbol << "," << std::setprecision(12) << t << std::setprecision(6) << ',' << level++ << ','
                   << (float) o.prc * tick_size << "," << o.vol << "," << o.order_id << "," << o.agent_id << ","
                   << std::setprecision(12) << o.t << std::setprecision(6) << "," << o.expiry
                   << "," << (char) o.side << ',' << time << endl;
        }
    }
}

deque<L2> Engine::get_l2_(double t, const string& time, unsigned int depth, const deque<lob_order>& lob, char side) {

    deque<L2> l2;
    unsigned int num = 0;
    unsigned int vol = 0;
    unsigned int prc = 0;
    unsigned int level = 0;
    for (const auto& o : lob) {

        if(prc == 0) {
            prc = o.prc;
            num = 1;
            vol = o.vol;
            continue;
        }

        if(prc != o.prc) {
            L2 _ord = { .t=t, .prc=prc, .vol=vol, .num=num, .side=side, .time=time};
            l2.push_back(_ord);

            level++;
            if (level >= depth)
                return l2;

            prc = o.prc;
            num = 1;
            vol = o.vol;
        }
        else {
            num++;
            vol += o.vol;
        }
    }

    if(vol > 0) {
        L2 _ord = {.t=t, .prc=prc, .vol=vol, .num=num, .side=side};
        l2.push_back(_ord);
    }

    return l2;
}

void Engine::save_l2(double t, const string& time, unsigned int depth) {

    deque<L2> buy_l2 = get_l2_(t, time, depth, buy_lob, 'B');
    deque<L2> sell_l2 = get_l2_(t, time, depth, sell_lob, 'S');

    int index = 0;
    for(const auto& o : buy_l2) {
        fs_lob_l2 << symbol << "," << std::setprecision(12) << o.t << std::setprecision(6) << "," << index++ << ","
                  << (float) o.prc * tick_size << "," << o.vol << "," << o.num << "," << o.side << ',' << time << endl;
    }

    index = 0;
    for(const auto& o : sell_l2) {
        fs_lob_l2 << symbol << "," << std::setprecision(12) << o.t << std::setprecision(6) << "," << index++ << ","
                  << (float) o.prc * tick_size << "," << o.vol << "," << o.num << "," << o.side << ',' << time << endl;
    }
}

void Engine::save_l1(double t, const string& time) {

    deque<L2> buy_l2 = get_l2_(t, time, 1, buy_lob, 'B');
    deque<L2> sell_l2 = get_l2_(t, time, 1, sell_lob, 'S');

    int total_vol = 0;
    for(const auto& o : buy_lob)
        total_vol += (int)o.vol;

    int depth = buy_lob.size();
    for(const auto& o : buy_l2) {
        fs_lob_l1 << symbol << "," << std::setprecision(12) << o.t << std::setprecision(6) << "," << (float) o.prc * tick_size << ","
                  << o.vol << "," << depth << "," << total_vol << "," << 'B' << ',' << time << "," << o.num << endl;
    }

    if(buy_l2.empty()) {
        fs_lob_l1 << symbol << "," << std::setprecision(12) << "" << std::setprecision(6) << "," << "" << ","
                  << "" << "," << 0 << "," << 0 << "," << 'B' << ',' << time << ",0" << endl;
    }

    total_vol = 0;
    for(const auto& o : sell_lob)
        total_vol += (int)o.vol;

    depth = sell_lob.size();
    for(const auto& o : sell_l2) {
        fs_lob_l1 << symbol << "," << std::setprecision(12) << o.t << std::setprecision(6) << "," << (float) o.prc * tick_size << ","
                  << o.vol << "," << depth << "," << total_vol << "," << 'S' << ',' << time << "," << o.num << endl;
    }

    if(sell_l2.empty()) {
        fs_lob_l1 << symbol << "," << std::setprecision(12) << "" << std::setprecision(6) << "," << "" << ","
                  << "" << "," << 0 << "," << 0 << "," << 'S' << ',' << time << ",0" << endl;
    }
}

/**
 * TODO: optimise.
 * @param step
 * @param time
 * @return an L1 report
 */
L1 Engine::get_l1(unsigned long int step, const string& time) {
    unsigned int * reference_prc = get_reference_prc();

    unsigned long int buy_total_vol = 0;
    for(const auto& o : buy_lob)
        buy_total_vol += o.vol;

    unsigned long int sell_total_vol = 0;
    for(const auto& o : sell_lob)
        sell_total_vol += o.vol;

    L1 l1 = {
        .best_bid_prc = reference_prc[0],
        .best_ask_prc = reference_prc[1],
        .best_bid_vol = buy_lob.empty() ? 0 : buy_lob.front().vol,
        .best_ask_vol = sell_lob.empty() ? 0 : sell_lob.front().vol,
        .total_bid_vol = buy_total_vol,
        .total_ask_vol = sell_total_vol,
        .step=step,
        .time=time,
        .symbol=symbol
    };

    return l1;
}