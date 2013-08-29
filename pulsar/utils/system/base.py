import ctypes
from select import select as _select
try:
    import signal
except ImportError:
    signal = None

from pulsar.utils.importer import import_module, module_attribute
from pulsar.utils.pep import iteritems, native_str

__all__ = ['ALL_SIGNALS',
           'SIG_NAMES',
           'SKIP_SIGNALS',
           'MAXFD',
           'set_proctitle',
           'set_owner_process']


SIG_NAMES = {}
MAXFD = 1024
SKIP_SIGNALS = frozenset(('KILL', 'STOP', 'WINCH'))

def all_signals():
    if signal:
        for sig in dir(signal):
            if sig.startswith('SIG') and sig[3] != "_":
                val = getattr(signal, sig)
                if isinstance(val, int):
                    name = sig[3:]
                    if name not in SKIP_SIGNALS:
                        SIG_NAMES[val] = name
                        yield name

            
ALL_SIGNALS = tuple(all_signals())


try:
    from setproctitle import setproctitle
    def set_proctitle(title):
        setproctitle(title)
        return True 
except ImportError: #pragma    nocover
    def set_proctitle(title):
        return


def set_owner_process(uid,gid):
    """ set user and group of workers processes """
    if gid:
        try:
            os.setgid(gid)
        except OverflowError:
            # versions of python < 2.6.2 don't manage unsigned int for
            # groups like on osx or fedora
            os.setgid(-ctypes.c_int(-gid).value)
            
    if uid:
        os.setuid(uid)
    