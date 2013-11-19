# Modified version of skiplist
# http://code.activestate.com/recipes/
#    576930-efficient-running-median-using-an-indexable-skipli/
import sys
from random import random
from math import log

ispy3k = int(sys.version[0]) >= 3

if not ispy3k:
    range = xrange


class Node(object):
    __slots__ = ('score', 'value', 'next', 'width')

    def __init__(self, score, value, next, width):
        self.score, self.value, self.next, self.width = (score, value,
                                                         next, width)


SKIPLIST_MAXLEVEL = 32     # Should be enough for 2^32 elements

neg_inf = float('-inf')
inf = float('inf')


class Skiplist(object):
    '''Sorted collection supporting O(log n) insertion,
    removal, and lookup by rank.'''

    def __init__(self, data=None, unique=False):
        self.unique = unique
        self.clear()
        if data is not None:
            self.extend(data)

    def clear(self):
        self.__size = 0
        self.__level = 1
        self.__head = Node('HEAD',
                           None,
                           [None]*SKIPLIST_MAXLEVEL,
                           [1]*SKIPLIST_MAXLEVEL)

    def __repr__(self):
        return list(self).__repr__()

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        return self.__size

    def __getitem__(self, index):
        node = self.__head
        traversed = 0
        index += 1
        for i in range(self.__level-1, -1, -1):
            while node.next[i] and (traversed + node.width[i]) <= index:
                traversed += node.width[i]
                node = node.next[i]
            if traversed == index:
                return node.value
        raise IndexError('skiplist index out of range')

    def extend(self, iterable):
        i = self.insert
        for score_values in iterable:
            i(*score_values)
    update = extend

    def rank(self, score):
        '''Return the 0-based index (rank) of ``score``.

        If the score is not available it returns a negative integer which
        absolute score is the left most closest index with score less than
        *score*.
        '''
        node = self.__head
        rank = 0
        for i in range(self.__level-1, -1, -1):
            while node.next[i] and node.next[i].score <= score:
                rank += node.width[i]
                node = node.next[i]
        if node.score == score:
            return rank - 1
        else:
            return -1 - rank

    def range(self, start=0, end=None, scores=False):
        N = len(self)
        if start < 0:
            start = max(N +  start, 0)
        if start >= N:
            raise StopIteration
        if end is None:
            end = N
        elif end < 0:
            end = max(N + end, 0)
        else:
            end = min(end, N)
        if start >= end:
            raise StopIteration
        node = self.__head.next[0]
        index = 0
        while node:
            if index >= start:
                if index < end:
                    yield (node.score, node.value) if scores else node.value
                else:
                    break
            index += 1
            node = node.next[0]

    def range_by_score(self, minvalue=neg_inf, maxvalue=inf):
        node = self.__head
        if start is not None:
            for i in range(self.__level-1, -1, -1):
                while node.next[i] and node.next[i].score <= start:
                    node = node.next[i]
        raise NotImplementedError

    def insert(self, score, value):
        # find first node on each level where node.next[levels].score > score
        if score != score:
            raise ValueError('Cannot insert score {0}'.format(score))
        chain = [None] * SKIPLIST_MAXLEVEL
        rank = [0] * SKIPLIST_MAXLEVEL
        node = self.__head
        for i in range(self.__level-1, -1, -1):
            #store rank that is crossed to reach the insert position
            rank[i] = 0 if i == self.__level-1 else rank[i+1]
            while node.next[i] and node.next[i].score <= score:
                rank[i] += node.width[i]
                node = node.next[i]
            chain[i] = node
        # the score already exist
        if chain[0].score == score and self.unique:
            return
        # insert a link to the newnode at each level
        level = min(SKIPLIST_MAXLEVEL, 1 - int(log(random(), 2.0)))
        if level > self.__level:
            for i in range(self.__level, level):
                rank[i] = 0
                chain[i] = self.__head
                chain[i].width[i] = self.__size
            self.__level = level

        # create the new node
        node = Node(score, value, [None]*level, [None]*level)
        for i in range(level):
            prevnode = chain[i]
            steps = rank[0] - rank[i]
            node.next[i] = prevnode.next[i]
            node.width[i] = prevnode.width[i] - steps
            prevnode.next[i] = node
            prevnode.width[i] = steps + 1

        # increment width for untouched levels
        for i in range(level, self.__level):
            chain[i].width[i] += 1

        self.__size += 1
        return node

    def remove(self, score):
        # find first node on each level where node.next[levels].score >= score
        chain = [None] * SKIPLIST_MAXLEVEL
        node = self.__head
        for i in range(self.__level-1, -1, -1):
            while node.next[i] and node.next[i].score < score:
                node = node.next[i]
            chain[i] = node

        node = node.next[0]
        if score != node.score:
            raise KeyError('Not Found')

        for i in range(self.__level):
            if chain[i].next[i] == node:
                chain[i].width[i] += node.width[i] - 1
                chain[i].next[i] = node.next[i]
            else:
                chain[i].width[i] -= 1

        self.__size -= 1

    def __iter__(self):
        'Iterate over values in sorted order'
        node = self.__head.next[0]
        while node:
            yield node.score, node.value
            node = node.next[0]

    def flat(self):
        return tuple(self._flat())

    def _flat(self):
        node = self.__head.next[0]
        while node:
            yield node.score
            yield node.value
            node = node.next[0]

