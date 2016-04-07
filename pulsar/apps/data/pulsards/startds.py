import asyncio

from pulsar import when_monitor_start, get_application, send
from pulsar.apps.data import create_store
from pulsar.apps.ds import PulsarDS


async def start_pulsar_ds(arbiter, host, workers=0):
    lock = getattr(arbiter, 'lock', None)
    if lock is None:
        arbiter.lock = lock = asyncio.Lock()
    await lock.acquire()
    try:
        app = await get_application('pulsards')
        if not app:
            app = PulsarDS(bind=host, workers=workers, load_config=False)
            cfg = await app(arbiter)
        else:
            cfg = app.cfg
        return cfg
    finally:
        lock.release()


async def start_store(app, url, workers=0, **kw):
    '''Equivalent to :func:`.create_store` for most cases excepts when the
    ``url`` is for a pulsar store not yet started.
    In this case, a :class:`.PulsarDS` is started.
    '''
    store = create_store(url, **kw)
    if store.name == 'pulsar':
        client = store.client()
        try:
            await client.ping()
        except ConnectionRefusedError:
            host = localhost(store._host)
            if not host:
                raise
            cfg = await send('arbiter', 'run', start_pulsar_ds,
                             host, workers)
            store._host = cfg.addresses[0]
            dns = store._buildurl()
            store = create_store(dns, **kw)
    app.cfg.set('data_store', store.dns)


def localhost(host):
    if isinstance(host, tuple):
        if host[0] in ('127.0.0.1', ''):
            return ':'.join((str(b) for b in host))
    else:
        return host


def _start_store(monitor):
    app = monitor.app
    if not isinstance(app, PulsarDS) and app.cfg.data_store:
        return start_store(app, app.cfg.data_store)

when_monitor_start.append(_start_store)
