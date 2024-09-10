//
// Created by Perukrishnen Vytelingum on 03/08/2020.
//

#ifndef ZEROINTELLIGENCEMARKET_SIMULATOR_H
#define ZEROINTELLIGENCEMARKET_SIMULATOR_H

#include <iostream>
#include <fstream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

#include "agentBase.h"
#include "ziTrader.h"
#include "fundamentalTrader.h"
#include "momentumTrader.h"
#include "hmomentumTrader.h"
#include "marketMaker.h"
#include "insTrader.h"
#include "spikeTrader.h"
#include "params.h"
#include "fundamentalValue.h"
#include "momentumValue.h"

using namespace std;
using namespace std::chrono;

const int TRADER_CATEGORY = 7;


/**
 * Single run of the market simulator.
 */
class simulator {
public:
    simulator(sim_params * simParams, const mc_params& mcParams);

    string simulationID;

    void run();

    virtual ~simulator() {
        simParams = nullptr;
    }

private:
    deque<agentBase *> agentPool;

    Engine * engine;

    sim_params * simParams;

    system_clock::time_point simDate;

    L1 previousL1;
    fundamentalValue * fv;
    unsigned long long total_steps;
    double totalMomentum = 0.;
    momentumValue * mv;
    fstream fs_pos;
    bool fs_pos_open = false;
    int positions[TRADER_CATEGORY];

    std::default_random_engine generator;

    std::uniform_int_distribution<int> * seed_dist = new std::uniform_int_distribution<int>(0, INT_MAX);

    void loadZITraders(unsigned int n_traders, zi_params ziParams);
    void loadFTraders(unsigned int n_traders, const ft_params& ftParams,
                      float tick_size);
    void loadMTraders(unsigned int n_traders, const mt_params& mtParams,
                      float tick_size);
    void loadHMTraders(unsigned int n_traders, const hmt_params& hmtParams,
                      float tick_size);
    void loadMarketMakers(unsigned int n_traders, const mm_params& mmParams,
                          float tick_size);
    void loadInsTrader(unsigned int n_traders, const ins_params& insParams);
    void loadSpikeTraders(unsigned int n_traders, const st_params& stParams);

    void update_fundamental_value(float fundamental_value);
    void calculate_total_momentum();

    system_clock::time_point get_market_open() {
        chrono::system_clock::time_point UTC_threshold_start = get_time_point("20201026 00:00",
                "%Y%m%d %H:%M", false);
        chrono::system_clock::time_point UTC_threshold_end = get_time_point("20210328 00:00",
                "%Y%m%d %H:%M", false);

        auto start = get_time(9, 0);
        if(start > UTC_threshold_start && start < UTC_threshold_end)
            start += std::chrono::hours(-1);

        return start;
    }


    system_clock::time_point get_time(int hour, int minute) {
        //auto t = system_clock::to_time_t(system_clock::now());

        std::time_t tt = std::chrono::system_clock::to_time_t(simDate);
        std::tm bt = *std::localtime(&tt);
        bt.tm_hour = hour;
        bt.tm_min = minute;
        bt.tm_sec = 0;

        return system_clock::from_time_t(std::mktime(&bt));
    }

    /**
     * Datetime Parser
     * @param timestamp
     * @return time_point
     */
    static system_clock::time_point get_time_point(const string& timestamp, const char* format, bool with_microseconds) {
        std::tm tm = {};
        std::stringstream ss1(timestamp);
        ss1 >> std::get_time(&tm, format);
        if (with_microseconds)
            return std::chrono::system_clock::from_time_t(
                    std::mktime(&tm)) + std::chrono::microseconds(std::stoi(timestamp.substr(19, 25)));

        return std::chrono::system_clock::from_time_t(std::mktime(&tm));
    }

    static string get_time_str(system_clock::time_point start_of_day, unsigned long int step,
                               unsigned long long step_size, const string& format) {
        system_clock::time_point t = start_of_day + microseconds(step * step_size);
        // system_clock::time_point t = start_of_day + milliseconds((int64_t)((double)(step) * (double)(step_size)/1000.0));
        std::time_t tt = std::chrono::system_clock::to_time_t(t);
        std::tm tm = *std::gmtime(&tt);
        std::stringstream ss;
        ss << std::put_time( &tm, format.c_str());
        ss << "." << std::setw(3) << std::setfill('0') << (step * step_size / 1000) % 1000;
        return ss.str();
    }

    void clearPosition(){
        std::fill(positions, positions+TRADER_CATEGORY, 0);
    }

    void computePosition(){
        int category;
        for (auto *a : agentPool){
            category = a->get_traderTypeNum();
            positions[category] += a->get_position();
        }
    }

    void outputPosition(double t, const string& time){
        for (int i = 0; i < TRADER_CATEGORY; i++){
            fs_pos << positions[i] << ",";
        }
        fs_pos << std::setprecision(12) << t << "," << time << endl;
    }

    void openPositionFs(const string& results_path){
        string filepath = results_path + "/";
        fs_pos.open(filepath + "agentPosition.csv", std::fstream::out);
        fs_pos_open = true;
        fs_pos << "ZIpos,FTpos,MTpos,HMTpos,MMpos,STpos,INSpos,step,time" << endl;
    }
};


#endif //ZEROINTELLIGENCEMARKET_SIMULATOR_H
