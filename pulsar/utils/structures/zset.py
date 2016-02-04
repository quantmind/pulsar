from .skiplist import Skiplist


class Zset:
    '''Ordered-set equivalent of redis zset.
    '''
    def __init__(self, data=None):
        self._sl = Skiplist()
        self._dict = {}
        if data:
            self.update(data)

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
                             in state.items()))

    def __eq__(self, other):
        if isinstance(other, Zset):
            return other._dict == self._dict
        return False

    def items(self):
        '''Iterable over ordered score, value pairs of this :class:`zset`
        '''
        return iter(self._sl)

    def range(self, start, end, scores=False):
        return self._sl.range(start, end, scores)

    def range_by_score(self, minval, maxval, include_min=True,
                       include_max=True, start=0, num=None, scores=False):
        return self._sl.range_by_score(minval, maxval, start=start,
                                       num=num, include_min=include_min,
                                       include_max=include_max,
                                       scores=scores)

    def score(self, member, default=None):
        '''The score of a given member'''
        return self._dict.get(member, default)

    def count(self, minval, maxval, include_min=True, include_max=True):
        return self._sl.count(minval, maxval, include_min, include_max)

    def add(self, score, val):
        r = 1
        if val in self._dict:
            sc = self._dict[val]
            if sc == score:
                return 0
            self.remove(val)
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
            score = self.remove(item)
            if score is not None:
                removed += 1
        return removed

    def remove(self, item):
        '''Remove ``item`` for the :class:`zset` it it exists.
        If found it returns the score of the item removed.
        '''
        score = self._dict.pop(item, None)
        if score is not None:
            index = self._sl.rank(score)
            assert index >= 0, 'could not find start range'
            for i, v in enumerate(self._sl.range(index)):
                if v == item:
                    assert self._sl.remove_range(index + i, index+i + 1) == 1
                    return score
            assert False, 'could not find element'

    def remove_range(self, start, end):
        '''Remove a range by score.
        '''
        return self._sl.remove_range(
            start, end, callback=lambda sc, value: self._dict.pop(value))

    def remove_range_by_score(self, minval, maxval,
                              include_min=True, include_max=True):
        '''Remove a range by score.
        '''
        return self._sl.remove_range_by_score(
            minval, maxval, include_min=include_min, include_max=include_max,
            callback=lambda sc, value: self._dict.pop(value))

    def clear(self):
        '''Clear this :class:`zset`.'''
        self._sl = Skiplist()
        self._dict.clear()

    def rank(self, item):
        '''Return the rank (index) of ``item`` in this :class:`zset`.'''
        score = self._dict.get(item)
        if score is not None:
            return self._sl.rank(score)

    def flat(self):
        return self._sl.flat()

    @classmethod
    def union(cls, zsets, weights, oper):
        result = None
        for zset, weight in zip(zsets, weights):
            if result is None:
                result = cls()
                sl = result._sl
                for score, value in zset._sl:
                    result.add(score*weight, value)
            else:
                for score, value in zset._sl:
                    score *= weight
                    existing = sl.score(value)
                    if existing is not None:
                        score = oper(score, existing)
                    result.add(score, value)
        return result

    @classmethod
    def inter(cls, zsets, weights, oper):
        result = None
        values = None
        for zset, _ in zip(zsets, weights):
            if values is None:
                values = set(zset)
            else:
                values.intersection_update(zset)
        #
        for zset, weight in zip(zsets, weights):
            if result is None:
                result = cls()
                for score, value in zset._sl:
                    if value in values:
                        result.add(score*weight, value)
            else:
                for score, value in zset._sl:
                    if value in values:
                        existing = result.score(value)
                        score = oper((score*weight, existing))
                        result.add(score, value)
        return result
