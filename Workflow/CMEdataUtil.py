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
