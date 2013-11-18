import sys
import array
import collections
import itertools

from ..pep import zip, iteritems


# Placeholder constants
TUPLE_LIMIT = 30



class Extra(object):
    __slots__ = ()

    def mget(self, fields):
        return [self.get(f) for f in fields]

    def flat(self):
        result = []
        [result.extend(pair) for pair in iteritems(self)]
        return result


class Dict(dict, Extra):
    pass


#class Hash(collections.MutableMapping, Extra):
class Hash:
    __slots__ = ('_data',)

    def __init__(self, *args, **kwargs):
        self._data = None
        self.update(*args, **kwargs)

    def __repr__(self):
        return self._data.__repr__()

    def __str__(self):
        return self._data.__str__()

    def __iter__(self):
        if isinstance(self._data, dict):
            return iter(self._dict)
        else:
            return ((k for k, _ in self._data))

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        if isinstance(self._data, dict):
            return self._data[key]
        else:
            for k, v in self._data:
                if k == key:
                    return v
            raise KeyError(key)

    def __setitem__(self, key, value):
        if isinstance(self._data, dict):
            self._data[key] = value
        else:
            data = dict(self._data)
            data[key] = value
            if len(data) < 30:
                data = tuple(iteritems(data))
            self._data = data

    def __delitem__(self, key):
        if isinstance(self._data, dict):
            del self._data[key]
        else:
            self._data = tuple(self._pop(key))

    def _pop(self, key):
        for k, value in self._data:
            if k == key:
                found = True
            else:
                yield k, value
        if not found:
            raise KeyError(key)

    def update(self, *args, **kwargs):
        if isinstance(self._data, dict):
            self._data.update(*args, **kwargs)
        else:
            if self._data:
                data = dict(self._data)
                data.data.update(*args, **kwargs)
            else:
                data = dict(*args, **kwargs)
            if len(data) < 30:
                data = tuple(iteritems(data))
            self._data = data

    def mget(self, fields):
        return [self.get(f) for f in fields]

    def flat(self):
        result = []
        [result.extend(pair) for pair in self.items()]
        return result
