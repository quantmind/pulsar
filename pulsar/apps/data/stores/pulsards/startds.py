from asyncio import Lock
try:
    from asyncio import ConnectionRefusedError
except (ImportError, NameError):
    pass

from pulsar import when_monitor_start, coroutine_return, get_application, send

from .base import create_store
from ...server import PulsarDS


def start_pulsar_ds(arbiter, host, workers=0):
    lock = getattr(arbiter, 'lock', None)
    if lock is None:
        arbiter.lock = lock = Lock()
    yield lock.acquire()
    try:
        app = yield get_application('pulsards')
        if not app:
            app = PulsarDS(bind=host, workers=workers)
            cfg = yield app(arbiter)
        else:
            cfg = app.cfg
        coroutine_return(cfg)
    finally:
        lock.release()


def start_store(url, workers=0, **kw):
    '''Equivalent to :func:`create_store` for most cases excepts when the
    ``url`` is for a pulsar store not yet started.

    In this case, the a :class:`.PulsarDS` is started.
    '''
    store = create_store(url, **kw)
    if store.name == 'pulsar':
        client = store.client()
        try:
            yield client.ping()
        except ConnectionRefusedError:
            host = localhost(store._host)
            if not host:
                raise
            cfg = yield send('arbiter', 'run', start_pulsar_ds, host, workers)
            store._host = cfg.addresses[0]
            dns = store._buildurl()
            store = create_store(dns, **kw)
    coroutine_return(store)


def localhost(host):
    if isinstance(host, tuple):
        if host[0] in ('127.0.0.1', ''):
            return ':'.join((str(b) for b in host))
    else:
        return host


def _start_store(monitor):
    app = monitor.app
    if not isinstance(app, PulsarDS):
        dns = app.cfg.data_store
        if dns:
            store = yield start_store(dns)
            app.cfg.set('data_store', store.dns)


when_monitor_start.append(_start_store)
