from .skiplist import Skiplist
from ..pep import iteritems


class Zset(object):
    '''Ordered-set equivalent of redis zset.
    '''
    def __init__(self):
        self._sl = Skiplist()
        self._dict = {}

    def __repr__(self):
        return repr(self._sl)
    __str__ = __repr__

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        for _, value in self._sl:
            yield value

    def __getstate__(self):
        return self._dict

    def __setstate__(self, state):
        self._dict = state
        self._sl = Skiplist(((score, member) for member, score
                             in iteritems(state)))

    def items(self):
        '''Iterable over ordered score, value pairs of this :class:`zset`
        '''
        return iter(self._sl)

    def range(self, start, end, scores=False):
        return self._sl.range(start, end, scores)

    def range_by_score(self, minval, maxval):
        return self._sl.range_by_score(minval, maxval)

    def score(self, member, default=None):
        '''The score of a given member'''
        return self._dict.get(member, default)

    def count(self, mmin, mmax):
        raise NotImplementedError

    def add(self, score, val):
        r = 1
        if val in self._dict:
            sc = self._dict[val]
            if sc == score:
                return 0
            self._sl.remove(sc)
            r = 0
        self._dict[val] = score
        self._sl.insert(score, val)
        return r

    def update(self, score_vals):
        '''Update the :class:`zset` with an iterable over pairs of
scores and values.'''
        add = self.add
        for score, value in score_vals:
            add(score, value)

    def remove_items(self, items):
        removed = 0
        for item in items:
            score = self._dict.pop(item, None)
            if score is not None:
                removed += 1
                self._sl.remove(score)
        return removed

    def remove(self, item):
        '''Remove ``item`` for the :class:`zset` it it exists.
If found it returns the score of the item removed.'''
        score = self._dict.pop(item, None)
        if score is not None:
            self._sl.remove(score)
            return score

    def clear(self):
        '''Clear this :class:`zset`.'''
        self._sl = skiplist()
        self._dict.clear()

    def rank(self, item):
        '''Return the rank (index) of ``item`` in this :class:`zset`.'''
        score = self._dict.get(item)
        if score is not None:
            return self._sl.rank(score)

    def flat(self):
        return self._sl.flat()
