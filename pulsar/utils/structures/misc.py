from copy import copy
from itertools import islice
import collections

Mapping = collections.Mapping


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
    if isinstance(value, list):
        return value
    if isgenerator(value) or isinstance(value, (tuple, set, frozenset)):
        return list(value)
    else:
        return [value]


class MultiValueDict(dict):
    """A subclass of dictionary customized to handle multiple
    values for the same key.
    """
    def __init__(self, data=None):
        super().__init__()
        if data:
            self.update(data)

    def __getitem__(self, key):
        """Returns the data value for this key.

        If the value is a list with only one element, it returns that element,
        otherwise it returns the list.
        Raises KeyError if key is not found.
        """
        l = super().__getitem__(key)
        return l[0] if len(l) == 1 else l

    def __setitem__(self, key, value):
        if key in self:
            l = super().__getitem__(key)
            # if value already there don't add it.
            # I'm not sure this is the correct way of doing thing but
            # it makes sense not to have repeating items
            if value not in l:
                l.append(value)
        else:
            super().__setitem__(key, [value])

    def __copy__(self):
        return self.__class__(((k, v[:]) for k, v in self.lists()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key, *arg):
        if key in self:
            l = super().pop(key)
            return l[0] if len(l) == 1 else l
        else:
            return super().pop(key, *arg)

    def getlist(self, key):
        """Returns the list of values for the passed key."""
        return super().__getitem__(key)

    def setlist(self, key, _list):
        if key in self:
            self.getlist(key).extend(_list)
        else:
            _list = aslist(_list)
            super().__setitem__(key, _list)

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self[key]

    def extend(self, key, values):
        """Appends an item to the internal list associated with key."""
        for value in values:
            self[key] = value

    def items(self):
        """Returns a generator of (key, value) pairs.
        """
        return ((key, self[key]) for key in self)

    def lists(self):
        """Returns a list of (key, list) pairs."""
        return super().items()

    def values(self):
        """Returns a list of the last value on every key list."""
        return [self[key] for key in self.keys()]

    def copy(self):
        return copy(self)

    def update(self, elem):
        if isinstance(elem, Mapping):
            elem = elem.items()
        for key, val in elem:
            self.extend(key, aslist(val))


class AttributeDictionary(collections.Mapping):
    '''A :class:`Mapping` structures which exposes keys as attributes.'''
    def __init__(self, *iterable, **kwargs):
        if iterable:
            if len(iterable) > 1:
                raise TypeError('%s exceped at most 1 arguments, got %s.' %
                                (self.__class__.__name__, len(iterable)))
            self.update(iterable[0])
        if kwargs:
            self.update(kwargs)

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

    def update(self, iterable):
        for name, value in mapping_iterator(iterable):
            setattr(self, name, value)

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
        l = list(self)
        try:
            index = l.index(pivot)
        except ValueError:
            pass
        else:
            l.insert(index, value)
            self.clear()
            self.extend(l)

    def insert_after(self, pivot, value):
        l = list(self)
        try:
            index = l.index(pivot)
        except ValueError:
            pass
        else:
            l.insert(index+1, value)
            self.clear()
            self.extend(l)

    def remove(self, elem, count=1):
        rev = False
        if count:
            if count < 0:
                rev = True
                count = -count
                l = list(reversed(self))
            else:
                l = list(self)
            while count:
                try:
                    l.remove(elem)
                    count -= 1
                except ValueError:
                    break
        else:
            l = [v for v in self if v != elem]
        removed = len(self) - len(l)
        if removed:
            self.clear()
            self.extend(reversed(l) if rev else l)
        return removed

    def trim(self, start, end):
        slice = list(islice(self, start, end))
        self.clear()
        self.extend(slice)


def merge_prefix(deque, size):
    """Replace the first entries in a deque of bytes with a single
string of up to *size* bytes."""
    if len(deque) == 1 and len(deque[0]) <= size:
        return
    prefix = []
    remaining = size
    while deque and remaining > 0:
        chunk = deque.popleft()
        if len(chunk) > remaining:
            deque.appendleft(chunk[remaining:])
            chunk = chunk[:remaining]
        prefix.append(chunk)
        remaining -= len(chunk)
    if prefix:
        deque.appendleft(b''.join(prefix))
    elif not deque:
        deque.appendleft(b'')


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
