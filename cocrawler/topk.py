'''
Various routines related to counting things
'''

from collections import namedtuple
from sortedcollections import ItemSortedDict


def getvaluevalue(k, v):
    return -v.value  # minus to invert sort


class topK_max:
    '''
    Given a stream of (key,value,ridealong) tuples, remember the k largest values.
    If a key is added repeatedly, use the largest value.
    '''
    def __init__(self, size):
        self.size = size
        self.element = namedtuple('topK_max_element', ['value', 'ridealong'])
        self.d = ItemSortedDict(getvaluevalue)

    def add(self, key, value, ridealong):
        if key in self.d:
            if value >= self.d[key].value:
                self.d[key] = self.element(value, ridealong)
        elif len(self.d) < self.size:
            self.d[key] = self.element(value, ridealong)
        elif value > self.d.peekitem()[1].value:
            self.d.popitem()
            self.d[key] = self.element(value, ridealong)

    def readout(self):
        return [(k, list(v)) for k, v in self.d.items()]

    # XXX to do: union, update


class topK_sum:
    '''
    Space-saving heavy hitters.

    Given a stream of (key, value) tuples, rememgber the k items
    with the largest sum of values.

    http://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf
    '''
    def __init__(self, size):
        self.size = size
        self.element = namedtuple('topK_max_element', ['value', 'ridealong', 'fake'])
        self.d = ItemSortedDict(getvaluevalue)

    def add(self, key, value, ridealong):
        if key in self.d:
            self.d[key] = self.element(self.d[key].value + value, ridealong, self.d[key].fake)
        elif len(self.d) < self.size:
            self.d[key] = self.element(value, ridealong, 0)
        elif value >= self.d.peekitem()[1].value:
            self.d.popitem()
            self.d[key] = self.element(value, ridealong, 0)
        else:
            evicted = self.d.popitem()
            oldvalue = evicted[1].value
            newvalue = max(value, oldvalue + 1)
            fake = max(newvalue - value, 0)
            self.d[key] = self.element(newvalue, ridealong, fake)

    def readout(self):
        ret = []
        for i in self.d.items():
            if i[1].value > 2 * i[1].fake:
                ret.append((i[0], [i[1].value, i[1].ridealong]))
        return ret

    # XXX to do: union, update


class topK_sum_hhh:
    '''
    Hierarchical heavy hitter.

    Given a stream of (key, (parts,), value) tuples, remember which
    parts have the largest sum of values.

    Example: given a list uf url paths within a website, remember the most popular ones
    '''


class topK_sum_hll:
    '''
    Heavy hitters with hyperloglog.

    Given a stream of (key, value) pairs, remember the k keys
    with the largest count of unique values. E.g. webpages
    in a site with most unique incoming external links
    '''
