import time

from .py2py3 import ispy3k, iteritems

__all__ = ['urlparse',
           'unquote',
           'urlsplit',
           'Headers',
           'bytes_to_str',
           'to_string']


if ispy3k:
    from urllib.parse import urlparse, unquote, urlsplit
    
    def bytes_to_str(b):
        return str(b, 'latin-1')
    
    def to_string(data):
        if isinstance(data, bytes):
            return str(data, 'latin-1')
        elif not isinstance(data,str):
            return str(data)
        return data
    
else:
    from urlparse import urlparse, unquote, urlsplit
    
    def bytes_to_str(b):
        return b
    
    def to_string(data):
        return str(data)

    
class Headers(object):
    
    def __init__(self, defaults=None):
        self._dict = {}
        self._keys = []
        if defaults is not None:
            self.extend(defaults)
    
    def __repr__(self):
        return self._dict.__repr__()
    __str__ = __repr__
    
    def extend(self, iterable):
        """Extend the headers with a dict or an iterable yielding keys and
        values.
        """
        if isinstance(iterable, dict):
            iterable = iteritems(iterable)
        for key, value in iterable:
            self.__setitem__(key, value)
    
    def __contains__(self, key):
        return key.lower() in self._dict
        
    def __iter__(self):
        d = self._dict
        for k in self._keys:
            yield k,d[k.lower()]

    def __len__(self):
        return len(self._keys)
    
    def __getitem__(self, key):
        return self._dict[key.lower()]

    def __setitem__(self, key, value):
        lkey = key.lower()
        if value:
            if isinstance(value, (tuple, list)):
                value = ','.join(value)
            if lkey in self._dict:
                value = ','.join((self._dict[lkey],value))
            else:
                self._keys.append(key)
            self._dict[lkey] = value
        
    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default
        
    def flat(self, version, status):
        vs = version + (status,)
        h = 'HTTP/{0}.{1} {2}'.format(*vs) 
        f = ''.join(("{0}: {1}\r\n".format(n, v) for n, v in self))
        return '{0}\r\n{1}\r\n'.format(h,f)
        