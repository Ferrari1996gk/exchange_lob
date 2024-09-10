import gzip
from datetime import datetime
import os
from CMEdataUtil import parseMessage

ticker = 'ESM0'
headline = "time,Prc,Qty,Aggressor,TotalQty,TickDirection\n"
print(len(headline.split(',')))

month = '04'
root = 'D:/projects/data/XCME/2010/%s/' % month
day_list = os.listdir(root)

for day in day_list:
    datestr = '2010' + month + day
    if datetime.strptime(datestr, '%Y%m%d').weekday() >= 5:
        print("Skip %s: weekend!"%datestr)
        continue
    print("Start parsing trading data for %s" % datestr)
    directory = 'D:/projects/data/XCME/2010/{0}/{1}/MD/XCME/'.format(month, day)
    name = '{0}-MD_xcme_es_fut_eth.gz'.format(datestr)
    file = directory + name

    fw = open(directory+ticker+'_trades.csv', 'w')
    fw.write(headline)

    f = gzip.open(file, 'rb')
    i = 0
    for line in f:
        if i % 500000 == 0:
            print(i)
        i += 1
        head, block_list = parseMessage(line.decode('utf-8')[:-1])
        if head is None:
            continue

        for block in block_list:
            if block['107'] != ticker or block['269'] != '2':
                continue

            prc, qty, total_qty = block['270'], block['271'], block['1020']
            aggressor = "" if block.get('5797') is None else block['5797']
            tickDirection = "" if block.get('274') is None else block['274']
            res_str = head['52'] + ",{0},{1},{2},{3},{4}\n".format(prc, qty, aggressor, total_qty, tickDirection)
            assert len(res_str.split(",")) == 6, "Error when generating result string!"
            fw.write(res_str)

    f.close()
    fw.close()
