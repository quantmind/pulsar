try:
    import signal
except ImportError:
    signal = None


__all__ = ['SIG_NAMES',
           'set_proctitle',
           'get_proctitle']


SIG_NAMES = {}

if signal:
    for sig in dir(signal):
        if sig.startswith('SIG') and sig[3] != "_":
            val = getattr(signal, sig)
            if isinstance(val, int):
                SIG_NAMES[val] = sig[3:]


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
