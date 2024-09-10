//
// Created by kgao_smd on 24/06/2021.
//

#ifndef ZEROINTELLIGENCEMARKET_MOMENTUMVALUE_H
#define ZEROINTELLIGENCEMARKET_MOMENTUMVALUE_H

class momentumValue {
public:
    momentumValue(const string& filepath) {
        fs.open(filepath + "/momentum_value.csv", std::fstream::out);
        fs << "timestamp,value" << '\n';
    }

    void update(unsigned int step, const string& time, const double momentum) {
        momentum_value = momentum;
         if(step % 10 == 0) //TODO Temporarily hardcoded for quick analysis
             fs << time << "," << std::setprecision(6) << to_string(momentum_value) << '\n';
    }

    void close() {
        if(fs.is_open())
            fs.close();
    }

    double get_value() const {
        return momentum_value;
    }

private:
    double momentum_value;
    fstream fs;
};


#endif //ZEROINTELLIGENCEMARKET_MOMENTUMVALUE_H
