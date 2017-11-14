from itertools import islice
import collections

Mapping = collections.Mapping


COLLECTIONS = (list, tuple, set, frozenset)


def mapping_iterator(iterable):
    if isinstance(iterable, Mapping):
        return iterable.items()
    else:
        return iterable or ()


def inverse_mapping(iterable):
    if isinstance(iterable, Mapping):
        iterable = iterable.items()
    return ((value, key) for key, value in iterable)


def isgenerator(value):
    return hasattr(value, '__iter__') and not hasattr(value, '__len__')


def aslist(value):
    if value is None:
        return []
    elif isinstance(value, list):
        return value
    elif isgenerator(value) or isinstance(value, COLLECTIONS):
        return list(value)
    else:
        return [value]


def as_tuple(value):
    if value is None:
        return ()
    elif isinstance(value, tuple):
        return value
    elif isgenerator(value) or isinstance(value, COLLECTIONS):
        return tuple(value)
    else:
        return value,


class AttributeDictionary(collections.Mapping):
    '''A :class:`Mapping` structures which exposes keys as attributes.'''
    def __init__(self, *iterable, **kwargs):
        if iterable:
            if len(iterable) > 1:
                raise TypeError('%s exceped at most 1 arguments, got %s.' %
                                (self.__class__.__name__, len(iterable)))
            self.update(iterable[0])
        if kwargs:
            self.__dict__.update(kwargs)

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

    def __contains__(self, name):
        return name in self.__dict__

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__)

    def __getattr__(self, name):
        return self.__dict__.get(name)

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    def __setitem__(self, name, value):
        self.__dict__[name] = value

    def __getitem__(self, name):
        return self.__dict__[name]

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, state):
        self.__dict__.update(state)

    def update(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

    def all(self):
        return self.__dict__

    def pop(self, name, default=None):
        return self.__dict__.pop(name, default)

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def copy(self):
        return self.__class__(self)

    def clear(self):
        self.__dict__.clear()


class FrozenDict(dict):
    '''A dictionary which cannot be changed once initialised.'''

    def __init__(self, *iterable, **kwargs):
        update = super().update
        if iterable:
            if len(iterable) > 1:
                raise TypeError('%s exceped at most 1 arguments, got %s.' %
                                (self.__class__.__name__, len(iterable)))
            update(iterable[0])
        if kwargs:
            update(kwargs)

    def __setitem__(self, key, value):
        raise TypeError("'%s' object does not support item assignment"
                        % self.__class__.__name__)

    def update(self, iterable):
        raise TypeError("'%s' object does not support update"
                        % self.__class__.__name__)

    def pop(self, key):
        raise TypeError("'%s' object does not support pop"
                        % self.__class__.__name__)

    def __gt__(self, other):
        if hasattr(other, '__len__'):
            return len(self) > len(other)
        else:
            return False

    def __lt__(self, other):
        if hasattr(other, '__len__'):
            return len(self) < len(other)
        else:
            return False


class Dict(dict):

    def mget(self, fields):
        return [self.get(f) for f in fields]

    def flat(self):
        result = []
        [result.extend(pair) for pair in self.items()]
        return result


class Deque(collections.deque):

    def insert_before(self, pivot, value):
        li = list(self)
        try:
            index = li.index(pivot)
        except ValueError:
            pass
        else:
            li.insert(index, value)
            self.clear()
            self.extend(li)

    def insert_after(self, pivot, value):
        li = list(self)
        try:
            index = li.index(pivot)
        except ValueError:
            pass
        else:
            li.insert(index+1, value)
            self.clear()
            self.extend(li)

    def remove(self, elem, count=1):
        rev = False
        if count:
            if count < 0:
                rev = True
                count = -count
                li = list(reversed(self))
            else:
                li = list(self)
            while count:
                try:
                    li.remove(elem)
                    count -= 1
                except ValueError:
                    break
        else:
            li = [v for v in self if v != elem]
        removed = len(self) - len(li)
        if removed:
            self.clear()
            self.extend(reversed(li) if rev else li)
        return removed

    def trim(self, start, end):
        slice = list(islice(self, start, end))
        self.clear()
        self.extend(slice)


def recursive_update(target, mapping):
    for key, value in mapping.items():
        if value is not None:
            if key in target:
                cont = target[key]
                if isinstance(value, Mapping) and isinstance(cont, Mapping):
                    recursive_update(cont, value)
                else:
                    target[key] = value
            else:
                target[key] = value
