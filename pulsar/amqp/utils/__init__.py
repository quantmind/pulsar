import pickle
import sys
import tempfile

from pprint import pformat
from time import sleep
from uuid import UUID, uuid4 as _uuid4, _uuid_generate_random

try:
    import ctypes
except ImportError:
    ctypes = None


def say(m, *s):
    sys.stderr.write(str(m) % s + "\n")


def uuid4():
    # Workaround for http://bugs.python.org/issue4607
    if ctypes and _uuid_generate_random:
        buffer = ctypes.create_string_buffer(16)
        _uuid_generate_random(buffer)
        return UUID(bytes=buffer.raw)
    return _uuid4()


def gen_unique_id():
    """Generate a unique id, having - hopefully - a very small chance of
    collission.

    For now this is provided by :func:`uuid.uuid4`.
    """
    return str(uuid4())


if sys.version_info >= (3, 0):
    def kwdict(kwargs):
        return kwargs
else:
    def kwdict(kwargs):
        """Make sure keyword arguments are not in unicode.

        This should be fixed in newer Python versions,
        see: http://bugs.python.org/issue4978.

        """
        return dict((key.encode("utf-8"), value)
                        for key, value in kwargs.items())


def maybe_list(v):
    if v is None:
        return []
    if hasattr(v, "__iter__"):
        return v
    return [v]




def emergency_dump_state(state, open_file=open, dump=pickle.dump):
    persist = tempfile.mktemp()
    say("EMERGENCY DUMP STATE TO FILE -> %s <-" % persist)
    fh = open_file(persist, "w")
    try:
        try:
            dump(state, fh, protocol=0)
        except Exception, exc:
            say("Cannot pickle state: %r. Fallback to pformat." % (exc, ))
            fh.write(pformat(state))
    finally:
        fh.flush()
        fh.close()
    return persist

############## str.partition/str.rpartition #################################


def _compat_rl_partition(S, sep, direction=None, reverse=False):
    items = direction(sep, 1)
    if len(items) == 1:
        if reverse:
            return '', '', items[0]
        return items[0], '', ''
    return items[0], sep, items[1]


def _compat_partition(S, sep):
    """``partition(S, sep) -> (head, sep, tail)``

    Search for the separator ``sep`` in ``S``, and return the part before
    it, the separator itself, and the part after it. If the separator is not
    found, return ``S`` and two empty strings.

    """
    return _compat_rl_partition(S, sep, direction=S.split)


def _compat_rpartition(S, sep):
    """``rpartition(S, sep) -> (tail, sep, head)``

    Search for the separator ``sep`` in ``S``, starting at the end of ``S``,
    and return the part before it, the separator itself, and the part
    after it. If the separator is not found, return two empty
    strings and ``S``.

    """
    return _compat_rl_partition(S, sep, direction=S.rsplit, reverse=True)


def partition(S, sep):
    if hasattr(S, 'partition'):
        return S.partition(sep)
    else:  # Python <= 2.4:
        return _compat_partition(S, sep)


def rpartition(S, sep):
    if hasattr(S, 'rpartition'):
        return S.rpartition(sep)
    else:  # Python <= 2.4:
        return _compat_rpartition(S, sep)
