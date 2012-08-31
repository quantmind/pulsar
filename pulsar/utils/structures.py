import sys
import weakref
from copy import copy
from collections import *

if sys.version_info < (2,7):
    from .fallbacks._collections import *
    
from .httpurl import mapping_iterator


def isgenerator(value):
    return hasattr(value,'__iter__') and not hasattr(value, '__len__')

def aslist(value):
    if isinstance(value, list):
        return value
    if isgenerator(value) or isinstance(value,(tuple,set,frozenset)):
        return list(value)
    else:
        return [value]


class WeakList(object):

    def __init__(self):
        self._list = []

    def append(self, obj):
        if obj:
            self._list.append(weakref.ref(obj))

    def remove(self, obj):
        wr = weakref.ref(obj)
        if wr in self._list:
            self._list.remove(wr)
            if wr not in self._list:
                return obj

    def __iter__(self):
        if self._list:
            ol = self._list
            nl = self._list = []
            for v in ol:
                obj = v()
                if obj:
                    nl.append(v)
                    yield obj
        else:
            raise StopIteration


class MultiValueDict(dict):
    """A subclass of dictionary customized to handle multiple
values for the same key.
    """
    def __init__(self, data=None):
        super(MultiValueDict, self).__init__()
        if data:
            self.update(data)

    def __getitem__(self, key):
        """Returns the data value for this key. If the value is a list with
only one element, it returns that element, otherwise it returns the list.
Raises KeyError if key is not found."""
        l = super(MultiValueDict, self).__getitem__(key)
        return l[0] if len(l) == 1 else l

    def __setitem__(self, key, value):
        if key in self:
            l = super(MultiValueDict, self).__getitem__(key)
            # if value already there don't add it.
            # I'm not sure this is the correct way of doing thing but
            # it makes sense not to have repeating items
            if value not in l:
                l.append(value)
        else:
            super(MultiValueDict, self).__setitem__(key, [value])

    def update(self, items):
        if isinstance(items, dict):
            items = iteritems(items)
        for k,v in items:
            self[k] = v

    def __copy__(self):
        return self.__class__(((k, v[:]) for k, v in self.lists()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key, *arg):
        if key in self:
            l = super(MultiValueDict, self).pop(key)
            return l[0] if len(l) == 1 else l
        else:
            return super(MultiValueDict, self).pop(key, *arg)

    def getlist(self, key):
        """Returns the list of values for the passed key."""
        return super(MultiValueDict, self).__getitem__(key)

    def setlist(self, key, _list):
        if key in self:
            self.getlist(key).extend(_list)
        else:
            _list = aslist(_list)
            super(MultiValueDict, self).__setitem__(key, _list)

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self[key]

    def extend(self, key, values):
        """Appends an item to the internal list associated with key."""
        for value in values:
            self[key] = value

    def items(self):
        """Returns a generator ovr (key, value) pairs,
where value is the last item in the list associated with the key.
        """
        return ((key, self[key]) for key in self)

    def lists(self):
        """Returns a list of (key, list) pairs."""
        return super(MultiValueDict, self).items()

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


class AttributeDictionary(object):
    
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
        
    def update(self, iterable):
        for name, value in mapping_iterator(iterable):
            setattr(self, name, value)
    
    def all(self):
        return self.__dict__
    
    def pop(self, name):
        return self.__dict__.pop(name, None)