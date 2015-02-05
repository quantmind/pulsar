import time
import string

try:
    pypy = True
    import __pypy__
except ImportError:
    pypy = False


identity = lambda x: x


default_timer = time.monotonic
string_type = str
ascii_letters = string.ascii_letters
is_string = lambda s: isinstance(s, str)
as_iterator = identity

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
