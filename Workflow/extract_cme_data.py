import gzip
from datetime import datetime
import os
from CMEdataUtil import MarketDepth, parseMessage

ticker = 'ESM0'
headline = "time,"+"".join(["BidPrc_%d,BidQty_%d,BidNum_%d,"%(i,i,i) for i in range(1, 11)]) + "".join(["AskPrc_%d,AskQty_%d,AskNum_%d,"%(i,i,i) for i in range(1, 10)]) + "AskPrc_10,AskQty_10,AskNum_10\n"
print(len(headline.split(',')))

month = '05'
root = 'D:/projects/data/XCME/2010/%s/' % month
day_list = os.listdir(root)

for day in day_list:
    datestr = '2010' + month + day
    if datetime.strptime(datestr, '%Y%m%d').weekday() >= 5:
        print("Skip %s: weekend!"%datestr)
        continue
    print("Start parsing data for %s"%datestr)
    directory = 'D:/projects/data/XCME/2010/{0}/{1}/MD/XCME/'.format(month, day)
    name = '{0}-MD_xcme_es_fut_eth.gz'.format(datestr)
    file = directory + name

    bid_depth = MarketDepth(total_level=10, side='bid')
    ask_depth = MarketDepth(total_level=10, side='ask')
    fw = open(directory+ticker+'.csv', 'w')
    fw.write(headline)

    f = gzip.open(file, 'rb')
    i = 0
    for line in f:
        flag = False
        if i % 500000 == 0:
            print(i)
        i += 1
        head, block_list = parseMessage(line.decode('utf-8')[:-1])
        if head is None:
            continue

        for block in block_list:
            if block['107'] != ticker or block.get('276') == 'C':
                continue
            if block['269'] == '0':
                flag = True
                field = (int(block['270']), int(block['271']), int(block['346']))
                level = int(block['1023'])
                if block['279'] == '0':
                    bid_depth.add(level, field)
                elif block['279'] == '1':
                    bid_depth.modify(level, field)
                elif block['279'] == '2':
                    bid_depth.delete(level, field[0])
                else:
                    raise Exception("Illegal tag 279!")
            elif block['269'] == '1':
                flag = True
                field = (int(block['270']), int(block['271']), int(block['346']))
                level = int(block['1023'])
                if block['279'] == '0':
                    ask_depth.add(level, field)
                elif block['279'] == '1':
                    ask_depth.modify(level, field)
                elif block['279'] == '2':
                    ask_depth.delete(level, field[0])
                else:
                    raise Exception("Illegal tag 279!")
            else:
                pass
        assert ask_depth.is_valid(), "Ask depth not valid!"
        assert bid_depth.is_valid(), "Bid depth not valid!"
        bid_str, ask_str = str(bid_depth), str(ask_depth)
        if flag and bid_str != "" and ask_str != "":
            assert bid_str[-1] == ask_str[-1] == "\n", "Error in depth string!"
            res_str = head['52'] + "," + bid_str[:-1] + "," + ask_str
            assert len(res_str.split(",")) == 61, "Error when generating result string!"
            fw.write(res_str)

    fw.close()
