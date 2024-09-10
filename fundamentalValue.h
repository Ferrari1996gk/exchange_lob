//
// Created by Anna on 03/03/2021.
//

#ifndef ZEROINTELLIGENCEMARKET_FUNDAMENTALVALUE_H
#define ZEROINTELLIGENCEMARKET_FUNDAMENTALVALUE_H

#include <cmath>
#include <random>
#include "params.h"

class fundamentalValue {
public:
    fundamentalValue(long seed_, const fundamental_value_params& fvParams_, const string& filepath):
                     seed(seed_), fvParams(fvParams_) {
        is_ready = fvParams.is_ready;
        if (is_ready){
            read_source_value();
        }else{
            fundamental_value = fvParams.s0;
            generator.seed(seed);
            normal = new std::normal_distribution<double>(.0, fvParams.sigma * std::sqrt(fvParams.step_size));
        }

        fs.open(filepath + "/fundamental_value.csv", std::fstream::out);
        fs << "timestamp,value" << '\n';
    }
    // enum{RANDOM_WALK, GBM}  fundamentalvalue = max(1(0.02), fundamentalvalue)
    void update(unsigned int step, const string& time) {
        if (is_ready){
            fundamental_value = source_value[step];
        }else{
            if (step != 0) {
                fundamental_value *= (float)exp((fvParams.mu - std::pow(fvParams.sigma, 2) / 2) *
                                                (float)fvParams.step_size + (*normal)(generator));
            }
        }

        if(step % fvParams.dump_freq == 0)
            fs << time << "," << std::setprecision(6) << to_string(fundamental_value) << '\n';
    }

    void read_source_value(){
        std::ifstream f(fvParams.source_path + "/source_fv.csv");
        if (f.good()) {
            source_length = 0;
            string line;
            while (getline(f, line)) {
                source_length += 1;
                source_value.push_back(std::stof(line));
            }
            f.close();
        }else{
            throw std::ios_base::failure("Exception when read source_fv.csv file!");
        }
    }

    bool ready() const{
        return is_ready;
    }

    unsigned long long get_source_length() const{
        return source_length;
    }

    void close() {
        if(fs.is_open())
            fs.close();
    }

    float get_value() const {
        return fundamental_value;
    }

private:
    long seed;
    float fundamental_value;
    bool is_ready;
    string source_file;
    vector<float> source_value;
    unsigned long long source_length = 0;
    fundamental_value_params fvParams;
    std::default_random_engine generator;
    std::normal_distribution<double>* normal;
    fstream fs;
};

#endif //ZEROINTELLIGENCEMARKET_FUNDAMENTALVALUE_H
