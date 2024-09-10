import gzip
import os
from datetime import datetime
from CMEdataUtil import MarketDepth, parseOrderMessage, OrderEvent, TradeEvent, update_levels, FillEvent

def block_to_order_indi1(block, head):
    return OrderEvent(head['60'], block['37'], block['55'], block['279'], block['269'], block['270'], 
                      block['37706'])

def block_to_order_indi4(block, level_block, head):
    return OrderEvent(head['60'], block['37'], level_block['55'], block['37708'], level_block['269'], 
                      level_block['270'], block['37706'])

def block_to_trade(block, head, agg_id, agg_qty):
    return TradeEvent(head['60'], block['55'], 'trade', block['5797'], block['270'], block['271'], agg_id, agg_qty, block['346'])
    
def write_orders(order_list, f):
    for o in order_list:
        f.write(str(o))


if __name__ == "__main__":

    orders_head = 'TRANSACTTIME,PUBLIC_ORDER_ID,SYMBOL,EXECTYPE,SIDE,PRICE,VISIBLEQTY\n'
    trades_head = 'TRANSACTTIME,SYMBOL,EXECTYPE,SIDE,EXECUTEDPRICE,EXECUTEDSIZE,PUBLICORDERID,PUBLICORDERQTY,ORDERSNUM\n'
    fills_head = 'TRANSACTTIME,PUBLICORDERID,PUBLICORDERQTY\n'
    levels_head = "time,"+"".join(["BidPrc_%d,BidQty_%d,BidNum_%d,"%(i,i,i) for i in range(1, 11)]) + "".join(["AskPrc_%d,AskQty_%d,AskNum_%d,"%(i,i,i) for i in range(1, 10)]) + "AskPrc_10,AskQty_10,AskNum_10\n"
    print(len(levels_head.split(',')))

    ticker = 'ESH2'
    year = '2022'
    month = '02'
    # day = '03'
    day_list = sorted(os.listdir('/mnt/ccnas2/bdp/kg1118/projects/data/CME/2022/%s'%month))
    print(day_list)
    for day in day_list:
        datestr = year + month + day
        # if datetime.strptime(datestr, '%Y%m%d').weekday() >= 5:
        #     print("Skip %s: weekend!"%datestr)
        #     continue
        print("Start parsing data for %s"%datestr)
        directory = '/mnt/ccnas2/bdp/kg1118/projects/data/CME/2022/{0}/{1}/MBO/XCME/'.format(month, day)
        name = '{0}-MBO_xcme_es_fut_eth.gz'.format(datestr)
        file = directory + name


        bid_depth = MarketDepth(total_level=10, side='bid')
        ask_depth = MarketDepth(total_level=10, side='ask')
        fl = open(directory+ticker+'_levels.csv', 'w')
        fl.write(levels_head)
        ft = open(directory+ticker+'_trades.csv', 'w')
        ft.write(trades_head)
        fo = open(directory+ticker+'_orders.csv', 'w')
        fo.write(orders_head)
        ff = open(directory+ticker+'_fills.csv', 'w')
        ff.write(fills_head)

        f = gzip.open(file, 'rb')
        i = 0

        for line in f:
            message = line.decode('utf-8')[:-1]
            if i % 500000 == 0:
                print(i)
            i += 1
            head, block_list = parseOrderMessage(message)
            if head is None:
                continue

            if head['000'] == '0':
                # trades
                assert len(block_list) == int(head['268']) + int(head['37705'])
                num_entries = int(head['268'])
                cumsum_id = num_entries
                flag = True # Used for the case when 5797=0 is in the middle of several blocks in one message
                for idx in range(num_entries):
                    block = block_list[idx]
                    assert block['279'] == '0'
                    if cumsum_id >= len(block_list): # For the case when there is some orders missing in trades message
                        print(i, 'cumsum_id larger than length of block_list!', cumsum_id, len(block_list))
                        break
                    if block['55'] == ticker:
                        left, right = cumsum_id, cumsum_id + int(block['346'])
                        for tmp_b in block_list[left:right]:
                            _fill = FillEvent(head['60'], ticker, tmp_b['37'], tmp_b['32'])
                            ff.write(str(_fill))

                        if block['5797'] == '0':
                            print(i, ' aggressor tag 5797 is 0 !!!')
                            flag = False
                            cumsum_id += int(block['346'])
                            continue
                        assert block_list[cumsum_id]['32'] == block['271']
                        t = block_to_trade(block, head, block_list[cumsum_id]['37'], block_list[cumsum_id]['32'])
                        ft.write(str(t))

                    cumsum_id += int(block['346'])

                if flag and cumsum_id != num_entries + int(head['37705']):
                    print(i, 'cumsum_id and length of block_list mismatch!', cumsum_id, num_entries + int(head['37705']))
            elif head['000'] == '1':
                assert int(head['268']) == len(block_list)
                for block in block_list:
                    if block['55'] == ticker and (block['269'] == '0' or block['269'] == '1'):
                        o = block_to_order_indi1(block, head)
                        fo.write(str(o))
                
            elif head['000'] == '2':
                assert len(block_list) == int(head['268']), "block number mismatch!"
                flag = update_levels(bid_depth, ask_depth, block_list, ticker)
                bid_str, ask_str = str(bid_depth), str(ask_depth)
                if flag and bid_str != "" and ask_str != "":
                    assert bid_str[-1] == ask_str[-1] == "\n", "Error in depth string!"
                    res_str = head['60'] + "," + bid_str[:-1] + "," + ask_str
                    assert len(res_str.split(",")) == 61, "Error when generating result string!"
                    fl.write(res_str)
                    
            elif head['000'] == '3':
                raise Exception('Unexpected value 3 for 000 in head')
            
            elif head['000'] == '4':
                assert len(block_list) == int(head['268']) + int(head['37705'])
                split = int(head['268'])
                flag = update_levels(bid_depth, ask_depth, block_list[:split], ticker)
                bid_str, ask_str = str(bid_depth), str(ask_depth)
                if flag and bid_str != "" and ask_str != "":
                    assert bid_str[-1] == ask_str[-1] == "\n", "Error in depth string!"
                    res_str = head['60'] + "," + bid_str[:-1] + "," + ask_str
                    assert len(res_str.split(",")) == 61, "Error when generating result string!"
                    fl.write(res_str)
                    
                for block in block_list[split:]:
                    level_block = block_list[int(block['9633'])-1]
                    if level_block['269'] != '0' and level_block['269'] != '1':
                        raise Exception('Invalid level block!')
                    if level_block['55'] == ticker:
                        o = block_to_order_indi4(block, level_block, head)
                        fo.write(str(o))
                
            else:
                raise Exception('Unknown value for 000 in head')
            
            

        f.close()
        fl.close()
        ft.close()
        fo.close()
        ff.close()