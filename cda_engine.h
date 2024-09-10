//
// Created by Perukrishnen Vytelingum on 06/04/2020.
//

#include <fstream>
#include <deque>

using namespace std;

enum class Side { BUY='B', SELL='S' };

enum class OrdType { LIMIT='L', MARKET='M', CANCEL='C', AMEND='A' };

enum class ExecutionReportType { NEW='L', CANCEL='C', REJECTED='R', FILL='F', PARTIAL_FILL='P', EXPIRED='E', AMEND='A' };

enum class TickFormat { SGX='S', LSE='L', None='-' };

class simuException: public exception {

    [[nodiscard]] const char* what() const noexcept override {
        return "simulation exception";
    }
};


struct L1 {
    unsigned int best_bid_prc;
    unsigned int best_ask_prc;
    unsigned int best_bid_vol;
    unsigned int best_ask_vol;
    unsigned long int total_bid_vol = 0;
    unsigned long int total_ask_vol = 0;
    unsigned long int step;
    string time;
    string symbol;

public:
    unsigned int get_spread() const {
        return best_ask_prc - best_bid_prc;
    }

    double get_mid_prc() const {
        return 0.5 * best_bid_prc + 0.5 * best_ask_prc;
    }

    unsigned int get_best_bid_prc() const {
        return best_bid_prc;
    }

    unsigned int get_best_ask_prc() const {
        return best_ask_prc;
    }

    // Compare two L1s.
    bool operator==(const L1 & rhs) const {
        return (
                (best_bid_prc == rhs.best_bid_prc) &&
                (best_ask_prc == rhs.best_ask_prc) &&
                (best_ask_vol == rhs.best_ask_vol) &&
                (best_bid_vol == rhs.best_bid_vol) &&
                (total_ask_vol == rhs.total_ask_vol) &&
                (total_bid_vol == rhs.total_bid_vol));
    }
} __attribute__((aligned(128))) __attribute__((packed));


/**
 * Trade Report - not that vwap is rounded to the tick size.
 */
struct Trade {
    float vwap;
    unsigned int vol;
    unsigned long int step;
    string time;
} __attribute__((aligned(64)));



/**
 * Queued order entry in Limit order book.
 * @param side is 'B' or 'S'.
 * @param order_type is 'L' for limit order, 'M' for market order and 'C' for cancel order.
 */
struct lob_order {
    unsigned int prc;
    unsigned int vol;
    string order_id;
    string agent_id;
    int agent_index = -1;
    double t;
    double expiry;
    Side side;
    OrdType orderType;
    string time;
    string traderType;
    string message;

    // Price-time priority.
    bool operator<(const lob_order & rhs) const {

        if(side != rhs.side)
            throw simuException();

        if (prc == rhs.prc)
            return t < rhs.t;

        if (side == Side::BUY)
            return prc > rhs.prc;
        else
            return prc < rhs.prc;
    }
} __attribute__((aligned(128)));


/**
 * Level 2 Market Data entry.
 */
struct L2 {
    double t;
    unsigned int prc;
    unsigned int vol;
    unsigned int num;
    char side;
    string time;
} __attribute__((aligned(64)));


/**
 * Transaction entry.
 */
struct transaction {
    unsigned int prc;
    unsigned int vol;
    string bid_id;
    string ask_id;
    string buyer_id;
    string seller_id;
    double t;
    string time;
} __attribute__((aligned(128))) __attribute__((packed));


struct execution_report  {
    ExecutionReportType executionReportType;
    unsigned int executedQuantity;
    unsigned int amendedQuantity;
    unsigned int executedPrice;
    string reason;
    double t;
    string time;
    string agent_id;

    Side side;
    unsigned int prc;
    unsigned int vol;
    string order_id;
    OrdType orderType;
    string traderType;

    int agent_index = -1;

} __attribute__((aligned(128))) __attribute__((packed));


/**
 * Exchange engine with simple protocols and price-time priority.
 */
class Engine {
public:
    Engine() = default;

    Engine(const string& symbol, const int * reference_price, float tick_size, const string& file_path,
            int verbose, const string& tick_format);

    void add_order(lob_order &o);
    void process_order(lob_order & o, deque<execution_report> * reports, deque<transaction> * transactions);

    bool expire(double t, deque<execution_report> * reports);

    void save_l1(double t, const string& time);
    void save_l2(double t, const string& time, unsigned int depth);
    void save_l3(double t, const string& time);

    L1 get_l1(unsigned long int step, const string& time);

    unsigned int * get_reference_prc();

    unsigned int reference_price[2] = {0, 0};

    unsigned long int sequence = 0;
    unsigned long int numTransactions = 0;

    // Limit order book
    deque<lob_order> buy_lob;
    deque<lob_order> sell_lob;

    virtual ~Engine() {
        if(fs_lob.is_open())
            fs_lob.close();

        if(fs_orders.is_open())
            fs_orders.close();

        if(fs_lob_l1.is_open())
            fs_lob_l1.close();

        if(fs_lob_l2.is_open())
            fs_lob_l2.close();

        if(fs_trades.is_open())
            fs_trades.close();

        if(fs_tick_orders.is_open())
            fs_tick_orders.close();

        if(fs_tick_trades.is_open())
            fs_tick_trades.close();

        if(fs_orders_cancelled.is_open())
            fs_orders_cancelled.close();
    }

private:

    float tick_size;

    TickFormat tick_format;

    int verbose;

    string symbol;

    unsigned int min_prc;
    unsigned int max_prc;

    fstream fs_lob;
    fstream fs_orders;
    fstream fs_tick_orders;
    fstream fs_tick_trades;
    fstream fs_lob_l1;
    fstream fs_lob_l2;
    fstream fs_trades;
    fstream fs_orders_cancelled;

    void clear_order(lob_order &o, deque<execution_report> * reports, deque<transaction> * transactions, bool is_amend);
    void cancel_order(lob_order &o, deque<execution_report> * reports, bool is_amend, bool * is_rejected);

    void clear_ask(lob_order &ask, deque<execution_report> * reports, deque<transaction> * transactions, bool is_amend);
    void clear_bid(lob_order &bid, deque<execution_report> * reports, deque<transaction> * transactions, bool is_amend);

    void update_reference_prc();

    static deque<L2> get_l2_(double t, const string& time, unsigned int depth, const deque<lob_order>& lob,
                             char side);

    static TickFormat get_tick_format(const string& format) {
        if(format == "SGX")
            return TickFormat::SGX;

        if(format == "LSE")
            return TickFormat::LSE;

        return TickFormat::None;
    }

    unsigned int get_ob_position(const string& order_id, Side side) {

        auto lob = side == Side::BUY ? buy_lob : sell_lob;

        unsigned int ob_position = 0;
        for(const auto& o : lob) {
            if(o.order_id == order_id)
                return ob_position;
            else
                ob_position++;
        }

        return lob.size();
    }

    unsigned int get_order_vol(const string& order_id, Side side) {

        auto lob = side == Side::BUY ? buy_lob : sell_lob;
        for(auto& o : lob) {
            if(o.order_id == order_id)
                return o.vol;
        }

        return 0;
    }
};
