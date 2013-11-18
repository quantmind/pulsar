# Cython version of skiplist with O(log n) updates
# Original author: Raymond Hettinger
# Original cython version: Wes McKinney
# Original license: MIT
# Link: http://code.activestate.com/recipes/576930/
from random import random


cdef class Node:

    cdef public:
        double score
        object value
        list next
        ndarray width_arr

    cdef:
        int *width

    def __init__(self, double score, object value, list next, ndarray width):
        self.score = score
        self.value = value
        self.next = next
        self.width_arr = width
        self.width = <int *> width.data


cdef int SKIPLIST_MAXLEVEL = 32
NIL = Node(np.inf, [], np.array([])) # Singleton terminator node


cdef class skiplistiterator:

    cdef:
        Node node

    def __init__(self, node):
        self.node = node

    def __iter__(self):
        return self

    def __next__(self):
        cdef Node node
        node = self.node.next[0]
        if node is not NIL:
            self.node = node
            return (node.value, node.value)
        else:
            raise StopIteration


cdef class Skiplist:
    '''
    Sorted collection supporting O(lg n) insertion, removal, and lookup by rank.
    '''
    cdef:
        int size
        int level
        bool unique
        Node head

    def __repr__(self):
        return list(self).__repr__()

    def __str__(self):
        return self.__repr__()

    def __init__(self, data=None, bool unique=False):
        self.size = 0
        self.level = 1
        self.unique = unique
        self.head = Node(np.NaN,
                         None,
                         [NIL] * SKIPLIST_MAXLEVEL,
                         np.ones(SKIPLIST_MAXLEVEL, dtype=int))

    def __len__(self):
        return self.size

    def __getitem__(self, index):
        cdef int i, traversed
        cdef Node node
        node = self.head
        traversed = 0
        index += 1
        for i in range(self.level-1, -1, -1):
            while node.next[i] and (traversed + node.width[i]) <= index:
                traversed += node.width[i]
                node = node.next[i]
            if traversed == index:
                return node.value
        raise IndexError('skiplist index out of range')

    def clear(self):
        self.size = 0
        self.head = Node(np.NaN,
                         None,
                         [NIL] * SKIPLIST_MAXLEVEL,
                         np.ones(SKIPLIST_MAXLEVEL, dtype=int))

    def rank(self, double score):
        '''Return the 0-based index (rank) of ``score``.

        If the score is not available it returns a negative integer which
        absolute score is the left most closest index with score less than
        *score*.
        '''
        cdef int i, rank
        cdef Node node

        node = self.head
        rank = 0
        for i in range(self.__level-1, -1, -1):
            while node.next[i] and node.next[i].score <= score:
                rank += node.width[i]
                node = node.next[i]
        if node.score == score:
            return rank - 1
        else:
            return -1 - rank

    def extend(self, iterable):
        i = self.insert
        for score_values in iterable:
            i(*score_values)

    def insert(self, double score, object value):
        cdef int i, steps, level
        cdef Node node, prevnode, newnode, next_at_level
        cdef list chain, steps_at_level

        # find first node on each level where node.next[levels].score > score
        if score != score:
            raise ValueError('Cannot insert score %s' % score)

        chain = [None] * SKIPLIST_MAXLEVEL
        rank = [0] * SKIPLIST_MAXLEVEL
        node = self.head

        for i in range(self.__level-1, -1, -1):
            #store rank that is crossed to reach the insert position
            rank[i] = 0 if i == self.__level-1 else rank[i+1]
            while node.next[i] and node.next[i].score <= score:
                rank[i] += node.width[i]
                node = node.next[i]
            chain[i] = node
        # the score already exist
        if chain[0].score == score and self.unique:
            return False
        # insert a link to the newnode at each level
        level = min(SKIPLIST_MAXLEVEL, 1 - int(Log2(random())))
        if level > self.level:
            for i in range(self.level, level):
                rank[i] = 0
                chain[i] = self.__head
                chain[i].width[i] = self.size
            self.level = level

        # create the new node
        newnode = Node(score, value, [None]*level, np.empty(level, dtype=int))
        for i in range(level):
            prevnode = chain[i]
            steps = rank[0] - rank[i]
            newnode.next[i] = prevnode.next[i]
            newnode.width[i] = prevnode.width[i] - steps
            prevnode.next[i] = newnode
            prevnode.width[i] = steps + 1

        # increment width for untouched levels
        for i in range(level, self.level):
            chain[i].width[i] += 1

        self.size += 1
        return True

    def __iter__(self):
        'Iterate over values in sorted order'
        return skiplistiterator(self.head)
