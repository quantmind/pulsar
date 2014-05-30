import sys
import time
import string
import threading
from inspect import istraceback


ispy3k = sys.version_info >= (3, 0)

if ispy3k:
    default_timer = time.monotonic
else:   # pragma    nocover
    default_timer = time.time

try:
    pypy = True
    import __pypy__
except ImportError:
    pypy = False

if ispy3k:  # Python 3
    import pickle
    string_type = str
    ascii_letters = string.ascii_letters
    zip = zip
    map = map
    range = range
    chr = chr
    iteritems = lambda d: d.items()
    itervalues = lambda d: d.values()
    is_string = lambda s: isinstance(s, str)

    def to_bytes(s, encoding=None, errors=None):
        '''Convert *s* into bytes'''
        if not isinstance(s, bytes):
            return ('%s' % s).encode(encoding or 'utf-8', errors or 'strict')
        elif not encoding or encoding == 'utf-8':
            return s
        else:
            d = s.decode('utf-8')
            return d.encode(encoding, errors or 'strict')

    def to_string(s, encoding=None, errors='strict'):
        """Inverse of to_bytes"""
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8', errors)
        else:
            return str(s)

    def native_str(s, encoding=None):
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8')
        else:
            return s

    def force_native_str(s, encoding=None):
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8')
        elif not isinstance(s, str):
            return str(s)
        else:
            return s

else:   # pragma : no cover
    from itertools import izip as zip, imap as map
    import cPickle as pickle
    string_type = unicode
    ascii_letters = string.letters
    range = xrange
    chr = unichr
    iteritems = lambda d: d.iteritems()
    itervalues = lambda d: d.itervalues()
    is_string = lambda s: isinstance(s, basestring)

    def to_bytes(s, encoding=None, errors='strict'):
        encoding = encoding or 'utf-8'
        if isinstance(s, bytes):
            if encoding != 'utf-8':
                return s.decode('utf-8', errors).encode(encoding, errors)
            else:
                return s
        else:
            return unicode(s).encode(encoding, errors)

    def to_string(s, encoding=None, errors='strict'):
        """Inverse of to_bytes"""
        if isinstance(s, bytes):
            return s.decode(encoding or 'utf-8', errors)
        else:
            return unicode(s)

    def native_str(s, encoding=None):
        if isinstance(s, unicode):
            return s.encode(encoding or 'utf-8')
        else:
            return s

    def force_native_str(s, encoding=None):
        if isinstance(s, unicode):
            return s.encode(encoding or 'utf-8')
        elif not isinstance(s, str):
            return str(s)
        else:
            return s
