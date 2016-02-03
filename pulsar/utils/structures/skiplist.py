# Modified version of skiplist
# http://code.activestate.com/recipes/
#    576930-efficient-running-median-using-an-indexable-skipli/
from random import random
from math import log
from collections import Sequence


class Node:
    __slots__ = ('score', 'value', 'next', 'width')

    def __init__(self, score, value, next, width):
        self.score, self.value, self.next, self.width = (score, value,
                                                         next, width)


SKIPLIST_MAXLEVEL = 32     # Should be enough for 2^32 elements

neg_inf = float('-inf')
inf = float('inf')


class Skiplist(Sequence):
    '''Sorted collection supporting O(log n) insertion,
    removal, and lookup by rank.'''
    __slots__ = ('_unique', '_size', '_head', '_level')

    def __init__(self, data=None, unique=False):
        self._unique = unique
        self.clear()
        if data is not None:
            self.extend(data)

    def __repr__(self):
        return list(self).__repr__()

    def __str__(self):
        return self.__repr__()

    def __len__(self):
        return self._size

    def __getitem__(self, index):
        node = self._head
        traversed = 0
        index += 1
        for i in range(self._level-1, -1, -1):
            while node.next[i] and (traversed + node.width[i]) <= index:
                traversed += node.width[i]
                node = node.next[i]
            if traversed == index:
                return node.value
        raise IndexError('skiplist index out of range')

    def clear(self):
        '''Clear the container from all data.'''
        self._size = 0
        self._level = 1
        self._head = Node('HEAD', None,
                          [None]*SKIPLIST_MAXLEVEL,
                          [1]*SKIPLIST_MAXLEVEL)

    def extend(self, iterable):
        '''Extend this skiplist with an iterable over
        ``score``, ``value`` pairs.
        '''
        i = self.insert
        for score_values in iterable:
            i(*score_values)
    update = extend

    def rank(self, score):
        '''Return the 0-based index (rank) of ``score``.

        If the score is not available it returns a negative integer which
        absolute score is the right most closest index with score less than
        ``score``.
        '''
        node = self._head
        rank = 0
        for i in range(self._level-1, -1, -1):
            while node.next[i] and node.next[i].score < score:
                rank += node.width[i]
                node = node.next[i]
        node = node.next[0]
        if node and node.score == score:
            return rank
        else:
            return -2 - rank

    def range(self, start=0, end=None, scores=False):
        N = len(self)
        if start < 0:
            start = max(N + start, 0)
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
        node = self._head.next[0]
        index = 0
        while node:
            if index >= start:
                if index < end:
                    yield (node.score, node.value) if scores else node.value
                else:
                    break
            index += 1
            node = node.next[0]

    def range_by_score(self, minval, maxval, include_min=True,
                       include_max=True, start=0, num=None, scores=False):
        node = self._head
        if include_min:
            for i in range(self._level-1, -1, -1):
                while node.next[i] and node.next[i].score < minval:
                    node = node.next[i]
        else:
            for i in range(self._level-1, -1, -1):
                while node.next[i] and node.next[i].score <= minval:
                    node = node.next[i]
        node = node.next[0]
        if node and node.score >= minval:
            index = 0
            while node:
                index += 1
                if num is not None and index - start > num:
                    break
                if ((include_max and node.score > maxval) or
                        (not include_max and node.score >= maxval)):
                    break
                if index > start:
                    yield (node.score, node.value) if scores else node.value
                node = node.next[0]

    def insert(self, score, value):
        # find first node on each level where node.next[levels].score > score
        if score != score:
            raise ValueError('Cannot insert score {0}'.format(score))
        chain = [None] * SKIPLIST_MAXLEVEL
        rank = [0] * SKIPLIST_MAXLEVEL
        node = self._head
        for i in range(self._level-1, -1, -1):
            # store rank that is crossed to reach the insert position
            rank[i] = 0 if i == self._level-1 else rank[i+1]
            while node.next[i] and node.next[i].score <= score:
                rank[i] += node.width[i]
                node = node.next[i]
            chain[i] = node
        # the score already exist
        if chain[0].score == score and self._unique:
            return
        # insert a link to the newnode at each level
        level = min(SKIPLIST_MAXLEVEL, 1 - int(log(random(), 2.0)))
        if level > self._level:
            for i in range(self._level, level):
                rank[i] = 0
                chain[i] = self._head
                chain[i].width[i] = self._size
            self._level = level

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
        for i in range(level, self._level):
            chain[i].width[i] += 1

        self._size += 1
        return node

    def remove_range(self, start, end, callback=None):
        '''Remove a range by rank.

        This is equivalent to perform::

            del l[start:end]

        on a python list.
        It returns the number of element removed.
        '''
        N = len(self)
        if start < 0:
            start = max(N + start, 0)
        if start >= N:
            return 0
        if end is None:
            end = N
        elif end < 0:
            end = max(N + end, 0)
        else:
            end = min(end, N)
        if start >= end:
            return 0
        node = self._head
        index = 0
        chain = [None] * self._level
        for i in range(self._level-1, -1, -1):
            while node.next[i] and (index + node.width[i]) <= start:
                index += node.width[i]
                node = node.next[i]
            chain[i] = node
        node = node.next[0]
        initial = self._size
        while node and index < end:
            next = node.next[0]
            self._remove_node(node, chain)
            index += 1
            if callback:
                callback(node.score, node.value)
            node = next
        return initial - self._size

    def remove_range_by_score(self, minval, maxval, include_min=True,
                              include_max=True, callback=None):
        '''Remove a range with scores between ``minval`` and ``maxval``.

        :param minval: the start value of the range to remove
        :param maxval: the end value of the range to remove
        :param include_min: whether or not to include ``minval`` in the
            values to remove
        :param include_max: whether or not to include ``maxval`` in the
            scores to to remove
        :param callback: optional callback function invoked for each
            score, value pair removed.
        :return: the number of elements removed.
        '''
        node = self._head
        chain = [None] * self._level
        if include_min:
            for i in range(self._level-1, -1, -1):
                while node.next[i] and node.next[i].score < minval:
                    node = node.next[i]
                chain[i] = node
        else:
            for i in range(self._level-1, -1, -1):
                while node.next[i] and node.next[i].score <= minval:
                    node = node.next[i]
                chain[i] = node
        node = node.next[0]
        initial = self._size
        while node and node.score >= minval:
            if ((include_max and node.score > maxval) or
                    (not include_max and node.score >= maxval)):
                break
            next = node.next[0]
            self._remove_node(node, chain)
            if callback:
                callback(node.score, node.value)
            node = next
        return initial - self._size

    def count(self, minval, maxval, include_min=True, include_max=True):
        '''Returns the number of elements in the skiplist with a score
        between min and max.
        '''
        rank1 = self.rank(minval)
        if rank1 < 0:
            rank1 = -rank1 - 1
        elif not include_min:
            rank1 += 1
        rank2 = self.rank(maxval)
        if rank2 < 0:
            rank2 = -rank2 - 1
        elif include_max:
            rank2 += 1
        return max(rank2 - rank1, 0)

    def __iter__(self):
        'Iterate over values in sorted order'
        node = self._head.next[0]
        while node:
            yield node.score, node.value
            node = node.next[0]

    def flat(self):
        return tuple(self._flat())

    def _flat(self):
        node = self._head.next[0]
        while node:
            yield node.score
            yield node.value
            node = node.next[0]

    def _remove_node(self, node, chain):
        for i in range(self._level):
            if chain[i].next[i] == node:
                chain[i].width[i] += node.width[i] - 1
                chain[i].next[i] = node.next[i]
            else:
                chain[i].width[i] -= 1
        self._size -= 1


class SkipListSlice:
    __slots__ = ('sl', 'slice')

    def __init__(self, sl, slice):
        self.sl = sl
        self.slice = slice
