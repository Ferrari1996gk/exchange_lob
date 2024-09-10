from collections import deque


class MarketDepth:
    def __init__(self, total_level=10, side='bid'):
        self.total_level = total_level
        self.side = side
        self.depth = deque([0 for _ in range(total_level)], maxlen=total_level)

    def modify(self, level, field):
        if self.depth[level-1] != 0:
            assert self.depth[level-1][0] == field[0], "Price mismatch!"
        self.depth[level-1] = field

    def add(self, level, field):
        if level == 1:
            self.depth.appendleft(field)
        else:
            if len(self.depth) == self.total_level:
                self.depth.pop()
            self.depth.insert(level-1, field)

    def delete(self, level, price):
        if self.depth[level-1] != 0:
            assert self.depth[level-1][0] == price, "Delete price mismatch!"
        del self.depth[level-1]

    def __str__(self):
        res = ""
        if 0 in self.depth:
            return res
        if len(self.depth) == 0:
            return "," * 29 + "\n"
        elif len(self.depth) == self.total_level:
            for i in range(self.total_level-1):
                res += "%d,%d,%d," % (self.depth[i][0], self.depth[i][1], self.depth[i][2])
            res += "%d,%d,%d\n" % (self.depth[self.total_level-1][0], self.depth[self.total_level-1][1], self.depth[self.total_level-1][2])
            return res
        else:
            for i in range(len(self.depth)):
                res += "%d,%d,%d," % (self.depth[i][0], self.depth[i][1], self.depth[i][2])
            for i in range(self.total_level - 1 - len(self.depth)):
                res += ",,,"
            res += ",,\n"
            return res

    def is_valid(self):
        flag = True
        if len(self.depth) <= 1 or 0 in self.depth:
            return flag
        if self.side == 'bid':
            for i in range(len(self.depth) - 1):
                flag = flag and self.depth[i][0] > self.depth[i+1][0]
        else:
            for i in range(len(self.depth) - 1):
                flag = flag and self.depth[i][0] < self.depth[i+1][0]
        return flag


def parseMessage(message, separator='\x01', valueSeparator='='):
    if len(message) == 0 or message[-1] != separator:
        raise ValueError('FIX Message is invalid!')
    head = {}
    tmp_ind = message.find('268=')
    if tmp_ind == -1:
        print(message)
        return None, None
    # assert tmp_ind != -1, "TAG 268 not found!"
    split = message.find(separator, tmp_ind)
    n = 0
    while n < split:
        # Parse the next tag
        nextValueSeparator = message.index(valueSeparator, n)
        tag = message[n:nextValueSeparator]
        # Parse the next value
        nextSeparator = message.index(separator, nextValueSeparator)
        value = message[nextValueSeparator + 1:nextSeparator]
        n = nextSeparator + 1
        assert head.get(tag) is None, "Illegal duplicated values!"
        head[tag] = value
    assert n == split+1, "Unexpected n value!"
    assert message[n:n+4] == "279=", "tag 268 not followed by tag 279"
    block_list = []
    idx = -1
    while n < len(message):
        # Parse the next tag
        nextValueSeparator = message.index(valueSeparator, n)
        tag = message[n:nextValueSeparator]
        if tag == '279':
            block_list.append({})
            idx += 1
        # Parse the next value
        nextSeparator = message.index(separator, nextValueSeparator)
        value = message[nextValueSeparator + 1:nextSeparator]
        n = nextSeparator + 1
        assert block_list[idx].get(tag) is None, "Illegal duplicated values!"
        block_list[idx][tag] = value
    assert len(block_list) == int(head['268']), "Number of blocks mismatch!"
    return head, block_list


def get_head(message, separator, valueSeparator, split_ind):
    head = {}
    split = message.find(separator, split_ind)
    n = 0
    while n < split:
        # Parse the next tag
        nextValueSeparator = message.index(valueSeparator, n)
        tag = message[n:nextValueSeparator]
        # Parse the next value
        nextSeparator = message.index(separator, nextValueSeparator)
        value = message[nextValueSeparator + 1:nextSeparator]
        n = nextSeparator + 1
        assert head.get(tag) is None, "Illegal duplicated values!"
        head[tag] = value
    assert n == split+1, "Unexpected n value!"
    return head, n


def handle_price_and_order_trades(message, separator, valueSeparator, start_ind, head, n):
    new_split = message.find(separator, start_ind)
    block_list = []
    idx = -1
    while True:
        # Parse the next tag
        nextValueSeparator = message.index(valueSeparator, n)
        tag = message[n:nextValueSeparator]
        # Parse the next value
        nextSeparator = message.index(separator, nextValueSeparator)
        value = message[nextValueSeparator + 1:nextSeparator]
        n = nextSeparator + 1
        if tag == '37705':
            head[tag] = value
            break
        if tag == '279':
            block_list.append({})
            idx += 1
        assert block_list[idx].get(tag) is None, "Illegal duplicated values!"
        block_list[idx][tag] = value
    assert len(block_list) == int(head['268']), "Number of blocks mismatch!"
    assert n == new_split + 1, "Unexpected n value!"
    assert message[n:n+3] == "37=", "tage 37 not follwed by tag 37705"
    while  n < len(message):
        # Parse the next tag
        nextValueSeparator = message.index(valueSeparator, n)
        tag = message[n:nextValueSeparator]
        # Parse the next value
        nextSeparator = message.index(separator, nextValueSeparator)
        value = message[nextValueSeparator + 1:nextSeparator]
        n = nextSeparator + 1
        if tag == '37':
            block_list.append({})
            idx += 1
        assert block_list[idx].get(tag) is None, "Illegal duplicated values!"
        block_list[idx][tag] = value
    assert len(block_list) == int(head['268']) + int(head['37705']), "Number of blocks mismatch!"
    return head, block_list

def handle_others(message, separator, valueSeparator, head, n):
    block_list = []
    idx = -1
    while n < len(message):
        # Parse the next tag
        nextValueSeparator = message.index(valueSeparator, n)
        tag = message[n:nextValueSeparator]
        if tag == '279':
            block_list.append({})
            idx += 1
        # Parse the next value
        nextSeparator = message.index(separator, nextValueSeparator)
        value = message[nextValueSeparator + 1:nextSeparator]
        n = nextSeparator + 1
        assert block_list[idx].get(tag) is None, "Illegal duplicated values!"
        block_list[idx][tag] = value
    assert len(block_list) == int(head['268']), "Number of blocks mismatch!"
    return head, block_list

def parseOrderMessage(message, separator='\x01', valueSeparator='='):
    if len(message) == 0 or message[-1] != separator:
        raise ValueError('FIX Message is invalid!')
    tmp_ind = message.find('268=')
    if tmp_ind == -1:
        print(message)
        return None, None
    # assert tmp_ind != -1, "TAG 268 not found!"
    head, n = get_head(message, separator, valueSeparator, tmp_ind)
    assert message[n:n+4] == "279=", "tag 268 not followed by tag 279"
    
    trade_ind = message.find('269=2')
    if trade_ind != -1:
        head['000'] = '0'
        # trades message logic
        detail_ind = message.find('37705=')
        assert detail_ind != -1
        head, block_list = handle_price_and_order_trades(message, separator, valueSeparator, detail_ind, head, n)
        return head, block_list
    
    level_ind = message.find('1023=')
    order_ind = message.find('37705=')
    if level_ind != -1 and order_ind != -1:
        head['000'] = '4'
        head, block_list = handle_price_and_order_trades(message, separator, valueSeparator, order_ind, head, n)
        return head, block_list
    else:
        if level_ind == -1 and order_ind == -1:
            head['000'] = '1'
        elif level_ind != -1:
            head['000'] = '2'
        else:
            head['000'] = '3'
        head, block_list = handle_others(message, separator, valueSeparator, head, n)
        return head, block_list


def update_levels(bid_depth, ask_depth, block_list, ticker):
    flag = False
    for block in block_list:
        if block['55'] != ticker:
            continue
        if block['269'] == '0':
            flag = True
            field = (float(block['270']), int(block['271']), int(block['346']))
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
            field = (float(block['270']), int(block['271']), int(block['346']))
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
    return flag

class OrderEvent:
    def __init__(self, trans_time, order_id, symbol, exec_type, side, price, qty):
        # All string type
        self.trans_time = trans_time
        self.order_id = order_id
        self.symbol = symbol
        self.exec_type = exec_type
        self.side = side
        self.price = price
        self.qty = qty
        
    def __str__(self):
        res = "%s,%s,%s,%s,%s,%s,%s\n" % (self.trans_time, self.order_id, self.symbol, self.exec_type, 
                                          self.side, self.price, self.qty)
        return res

class FillEvent:
    def __init__(self, trans_time, symbol, order_id, trans_qty):
        self.trans_time = trans_time
        self.symbol = symbol
        self.order_id = order_id
        self.trans_qty = trans_qty
        
    def __str__(self):
        res = "%s,%s,%s,%s\n" % (self.trans_time, self.symbol, self.order_id, self.trans_qty)
        return res
    
class TradeEvent:
    def __init__(self, trans_time, symbol, exec_type, side, price, exec_size, aggressor_id, aggressor_qty, orders_num):
        self.trans_time = trans_time
        self.symbol = symbol
        self.exec_type = exec_type
        self.side = side
        self.price = price
        self.exec_size = exec_size
        self.aggressor_id = aggressor_id
        self.aggressor_qty = aggressor_qty
        self.orders_num = orders_num
    
    def __str__(self):
        res = "%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (self.trans_time, self.symbol, self.exec_type, self.side, self.price, 
                                                self.exec_size, self.aggressor_id, self.aggressor_qty, self.orders_num)
        return res
