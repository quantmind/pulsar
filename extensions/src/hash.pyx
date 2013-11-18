import sys
from collections import Mapping
from array import array


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

else:
    def mstr(s):
        if isinstance(s, (bytes, unicode)):
            return s
        else:
            return str(s)


def _gen_probes(hashvalue, mask):
    'Same sequence of probes used in the current dictionary design'
    PERTURB_SHIFT = 5
    if hashvalue < 0:
        hashvalue = -hashvalue
    i = hashvalue & mask
    yield i
    perturb = hashvalue
    while True:
        i = (5 * i + perturb + 1) & 0xFFFFFFFFFFFFFFFF
        yield i & mask
        perturb >>= PERTURB_SHIFT


def _make_index(n):
    'New sequence of indices using the smallest possible datatype'
    if n <= 2**7:
        return array('b', [FREE]) * n       # signed char
    if n <= 2**15:
        return array('h', [FREE]) * n      # signed short
    if n <= 2**31:
        return array('l', [FREE]) * n      # signed long
    return [FREE] * n   # python integers


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


cdef class Hash(dict):
    '''Space efficient dictionary with fast iteration and cheap resizes.

    Originally from Raymond Hettinger recipe

    http://code.activestate.com/recipes/578375/
    '''
    def __init__(self, *args, **kwds):
        self.indices = _make_index(8)
        self.hashlist = []
        self.keylist = []
        self.valuelist = []
        self.used = 0
        self.filled = 0
        self.update(*args, **kwds)

    cdef _lookup(self, key, int hashvalue):
        'Same lookup logic as currently used in real dicts'
        assert self.filled < len(self.indices)   # At least one open slot
        freeslot = None
        for i in _gen_probes(hashvalue, len(self.indices)-1):
            index = self.indices[i]
            if index == FREE:
                return (FREE, i) if freeslot is None else (DUMMY, freeslot)
            elif index == DUMMY:
                if freeslot is None:
                    freeslot = i
            elif (self.keylist[index] is key or
                  self.hashlist[index] == hashvalue
                  and self.keylist[index] == key):
                    return (index, i)

    def _resize(self, n):
        '''Reindex the existing hash/key/value entries.
           Entries do not get moved, they only get new indices.
           No calls are made to hash() or __eq__().

        '''
        n = 2 ** n.bit_length()                     # round-up to power-of-two
        self.indices = _make_index(n)
        for index, hashvalue in enumerate(self.hashlist):
            for i in _gen_probes(hashvalue, n-1):
                if self.indices[i] == FREE:
                    break
            self.indices[i] = index
        self.filled = self.used

    def clear(self):
        self.indices = _make_index(8)
        self.hashlist = []
        self.keylist = []
        self.valuelist = []
        self.used = 0
        self.filled = 0                                         # used + dummies

    def __getitem__(self, key):
        hashvalue = hash(key)
        index, i = self._lookup(key, hashvalue)
        if index < 0:
            raise KeyError(key)
        return self.valuelist[index]

    def __setitem__(self, key, value):
        hashvalue = hash(key)
        index, i = self._lookup(key, hashvalue)
        if index < 0:
            self.indices[i] = self.used
            self.hashlist.append(hashvalue)
            self.keylist.append(key)
            self.valuelist.append(value)
            self.used += 1
            if index == FREE:
                self.filled += 1
                if self.filled * 3 > len(self.indices) * 2:
                    self._resize(4 * len(self))
        else:
            self.valuelist[index] = value

    def __delitem__(self, key):
        hashvalue = hash(key)
        index, i = self._lookup(key, hashvalue)
        if index < 0:
            raise KeyError(key)
        self.indices[i] = DUMMY
        self.used -= 1
        # If needed, swap with the lastmost entry to avoid leaving a "hole"
        if index != self.used:
            lasthash = self.hashlist[-1]
            lastkey = self.keylist[-1]
            lastvalue = self.valuelist[-1]
            lastindex, j = self._lookup(lastkey, lasthash)
            assert lastindex >= 0 and i != j
            self.indices[j] = index
            self.hashlist[index] = lasthash
            self.keylist[index] = lastkey
            self.valuelist[index] = lastvalue
        # Remove the lastmost entry
        self.hashlist.pop()
        self.keylist.pop()
        self.valuelist.pop()

    def __len__(self):
        return self.used

    def __iter__(self):
        return iter(self.keylist)

    def keys(self):
        return list(self.keylist)

    def values(self):
        return list(self.valuelist)

    def items(self):
        return zip(self.keylist, self.valuelist)

    def __contains__(self, key):
        index, i = self._lookup(key, hash(key))
        return index >= 0

    def get(self, key, default=None):
        index, i = self._lookup(key, hash(key))
        return self.valuelist[index] if index >= 0 else default

    def popitem(self):
        if not self.keylist:
            raise KeyError('popitem(): dictionary is empty')
        key = self.keylist[-1]
        value = self.valuelist[-1]
        del self[key]
        return key, value

    def __repr__(self):
        return 'Hash(%s)' % ', '.join(('%r: %r' % v for v in self.items()))
    __str__ = __repr__

    def mget(self, fields):
        return [self.get(f) for f in fields]

    def flat(self):
        # TODO: this is slow, make it faster
        result = []
        [result.extend(pair) for pair in self.items()]
        return result

    def show_structure(self, stdout=None):
        'Diagnostic method.  Not part of the API.'
        stdout = stdout or sys.stdout
        stdout.write('=' * 50)
        stdout.write('\n')
        stdout.write('%s\n' % self)
        stdout.write('Indices: %r\n' % self.indices)
        for i, row in enumerate(zip(self.hashlist, self.keylist,
                                    self.valuelist)):
            stdout.write('%d  %r' % (i, row))
        stdout.write('-' * 50)
        stdout.write('\n')
