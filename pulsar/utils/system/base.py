try:
    import signal
except ImportError:
    signal = None


__all__ = ['ALL_SIGNALS',
           'SIG_NAMES',
           'SKIP_SIGNALS',
           'MAXFD',
           'set_proctitle',
           'get_proctitle']


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
    from setproctitle import setproctitle, getproctitle

    def set_proctitle(title):
        setproctitle(title)
        return True

    def get_proctitle():
        return getproctitle()

except ImportError:  # pragma    nocover

    def set_proctitle(title):
        return False

    def get_proctitle():
        pass
