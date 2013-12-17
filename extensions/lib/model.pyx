import sys
from collections import Mapping, MutableMapping

cimport common

# Placeholder constants
FREE = -1
DUMMY = -2
ispy3k = sys.version_info >= (3, 0)

if ispy3k:
    def mstr(s):
        if isinstance(s, bytes):
            return s.decode('utf-8')
        elif not isinstance(s, str):
            return str(s)
        else:
            return s

    def iteritems(value):
        return value.items()
else:
    def mstr(s):
        if isinstance(s, (bytes, unicode)):
            return s
        else:
            return str(s)

    def iteritems(value):
        return value.iteritems()


cdef class Model(dict):
    cdef set _access_cache
    cdef int _modified

    def __cinit__(self):
        self._access_cache = set()
        self._modified = 0

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        self._modified = 0

    def __getitem__(self, field):
        field = mstr(field)
        value = super(Model, self).__getitem__(field)
        if field not in self._access_cache:
            self._access_cache.add(field)
            if field in self._meta.converters:
                value = self._meta.converters[field](value)
                super(Model, self).__setitem__(field, value)
        return value

    def get(self, field, default=None):
        try:
            return self.__getitem__(field)
        except KeyError:
            return default

    def __setitem__(self, field, value):
        field = mstr(field)
        self._access_cache.discard(field)
        super(Model, self).__setitem__(field, value)
        self._modified = 1

    def update(self, *args, **kwargs):
        if len(args) == 1:
            iterable = args[0]
            if isinstance(iterable, Mapping):
                iterable = iterable.items() if ispy3k else iterable.iteritems()
            super(Model, self).update(((mstr(k), v) for k, v in iterable))
            self._modified = 1
        elif args:
            raise TypeError('expected at most 1 arguments, got %s' % len(args))
        if kwargs:
            super(Model, self).update(**kwargs)
            self._modified = 1

    def modified(self):
        return self._modified > 0

    def clear(self):
        if self:
            self._modified = 1
            super(Model, self).clear()
