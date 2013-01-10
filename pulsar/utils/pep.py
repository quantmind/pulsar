import sys
import time
import threading

ispy3k = sys.version_info >= (3, 0)
ispy26 = sys.version_info < (2, 7)
ispy33 = sys.version_info >= (3, 3)

if ispy33:
    default_timer = time.monotonic
else:   #pragma    nocover
    default_timer = time.time
    
    
if ispy3k: # Python 3
    string_type = str
    ascii_letters = string.ascii_letters
    zip = zip
    map = map
    range = range
    chr = chr
    iteritems = lambda d : d.items()
    itervalues = lambda d : d.values()
    is_string = lambda s: isinstance(s, str)
    is_string_or_native_string = is_string

    def to_bytes(s, encoding=None, errors=None):
        errors = errors or 'strict'
        encoding = encoding or 'utf-8'
        if isinstance(s, bytes):
            if encoding != 'utf-8':
                return s.decode('utf-8', errors).encode(encoding, errors)
            else:
                return s
        else:
            return ('%s'%s).encode(encoding, errors)

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
    string_type = unicode
    ascii_letters = string.letters
    range = xrange
    chr = unichr
    iteritems = lambda d : d.iteritems()
    itervalues = lambda d : d.itervalues()
    is_string = lambda s: isinstance(s, unicode)
    
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
class EventLoopPolicy:
    """Abstract policy for accessing the event loop."""

    def get_event_loop(self):
        """XXX"""
        raise NotImplementedError

    def set_event_loop(self, event_loop):
        """XXX"""
        raise NotImplementedError

    def new_event_loop(self):
        """XXX"""
        raise NotImplementedError


class DefaultEventLoopPolicy(threading.local, EventLoopPolicy):
    """Default policy implementation for accessing the event loop.

    In this policy, each thread has its own event loop.  However, we
    only automatically create an event loop by default for the main
    thread; other threads by default have no event loop.

    Other policies may have different rules (e.g. a single global
    event loop, or automatically creating an event loop per thread, or
    using some other notion of context to which an event loop is
    associated).
    """
    _event_loop = None

    def get_event_loop(self):
        """Get the event loop.

        This may be None or an instance of EventLoop.
        """
        if (self._event_loop is None and
            threading.current_thread().name == 'MainThread'):
            self._event_loop = self.new_event_loop()
        return self._event_loop

    def set_event_loop(self, event_loop):
        """Set the event loop."""
        assert event_loop is None or isinstance(event_loop, EventLoop)
        self._event_loop = event_loop


# Event loop policy.  The policy itself is always global, even if the
# policy's rules say that there is an event loop per thread (or other
# notion of context).  The default policy is installed by the first
# call to get_event_loop_policy().
_event_loop_policy = None

def get_event_loop_policy():
    """XXX"""
    global _event_loop_policy
    if _event_loop_policy is None:
        _event_loop_policy = DefaultEventLoopPolicy()
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

def new_event_loop():
    """XXX"""
    return get_event_loop_policy().new_event_loop()
