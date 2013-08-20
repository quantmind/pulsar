import sys
import time
import string
import threading
from inspect import istraceback

ispy3k = sys.version_info >= (3, 0)
ispy26 = sys.version_info < (2, 7)
ispy33 = sys.version_info >= (3, 3)

if ispy33:
    default_timer = time.monotonic
else:   #pragma    nocover
    default_timer = time.time
    
    
if ispy3k: # Python 3
    import pickle
    string_type = str
    ascii_letters = string.ascii_letters
    zip = zip
    map = map
    range = range
    chr = chr
    iteritems = lambda d : d.items()
    itervalues = lambda d : d.values()
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

    def raise_error_trace(err, traceback):
        if istraceback(traceback):
            raise err.with_traceback(traceback)
        else:
            raise err
        
else:   # pragma : no cover
    from itertools import izip as zip, imap as map
    import cPickle as pickle
    from .fallbacks.py2 import *
    string_type = unicode
    ascii_letters = string.letters
    range = xrange
    chr = unichr
    iteritems = lambda d : d.iteritems()
    itervalues = lambda d : d.itervalues()
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
        
        
################################################################################
###    PEP 3156
###    These classes will be eventually replaced by the standard lib

class EventLoop(object):
    '''This is just a signature'''
    def run_in_executor(self, executor, callback, *args):
        raise NotImplementedError
    
    
class EventLoopPolicy:
    """Abstract policy for accessing the event loop."""

    def get_event_loop(self):
        """XXX"""
        raise NotImplementedError

    def set_event_loop(self, event_loop):
        """XXX"""
        raise NotImplementedError

    def new_event_loop(self, **kwargs):
        """XXX"""
        raise NotImplementedError


# Event loop policy.  The policy itself is always global, even if the
# policy's rules say that there is an event loop per thread (or other
# notion of context).  The default policy is installed by the first
# call to get_event_loop_policy().
_event_loop_policy = None

def get_event_loop_policy():
    """XXX"""
    global _event_loop_policy
    return _event_loop_policy

def set_event_loop_policy(policy):
    """XXX"""
    global _event_loop_policy
    assert policy is None or isinstance(policy, EventLoopPolicy)
    _event_loop_policy = policy

def get_event_loop():
    """XXX"""
    return get_event_loop_policy().get_event_loop()

def set_event_loop(event_loop):
    """XXX"""
    get_event_loop_policy().set_event_loop(event_loop)

def new_event_loop(**kwargs):
    """XXX"""
    return get_event_loop_policy().new_event_loop(**kwargs)
