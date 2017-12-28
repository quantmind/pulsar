'''
sequential
~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: sequential


ActorTestMixin
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ActorTestMixin
   :members:
   :member-order: bysource


check server
~~~~~~~~~~~~~~~~~~

.. autofunction:: check_server

'''
import logging
import unittest
import asyncio

from pulsar.api import get_actor, send, spawn, ImproperlyConfigured
from pulsar.utils.exceptions import format_traceback
from pulsar.async.futures import as_gather
from pulsar.apps.data import create_store


LOGGER = logging.getLogger('pulsar.test')


def skipUnless(condition, reason):
    """
    Skip a test unless the condition is true.
    """
    if hasattr(condition, '__call__'):
        def decorator(test_item):
            test_item.__unittest_async_skip_unless__ = condition
            test_item.__unittest_skip_why__ = reason
            return test_item

        return decorator
    else:
        return unittest.skipUnless(condition, reason)


def allowFailure(test):
    test.__unittest_allow_failure__ = True
    return test


async def skip_test(o):
    if hasattr(o, '__unittest_async_skip_unless__'):
        skip = not await o.__unittest_async_skip_unless__()
    else:
        skip = getattr(o, '__unittest_skip__', False)
    return skip


def skip_reason(o):
    return getattr(o, '__unittest_skip_why__', '')


def expecting_failure(o):
    return getattr(o, '__unittest_expecting_failure__', False)


def allow_failure(o):
    return getattr(o, '__unittest_allow_failure__', False)


class TestFailure:

    def __init__(self, exc):
        self.exc = exc
        self.trace = format_traceback(exc)

    def __str__(self):
        return '\n'.join(self.trace)


def sequential(cls):
    '''Decorator for a :class:`~unittest.TestCase` which cause
    its test functions to run sequentially rather than in an
    asynchronous fashion.

    Typical usage::

        import unittest

        from pulsar.apps.test import sequential

        @sequenatial
        class MyTests(unittest.TestCase):
            ...

    You can also run test functions sequentially when using the
    :ref:`sequential <apps-test-sequential>` flag in the command line.
    '''
    cls._sequential_execution = True
    return cls


class test_timeout:

    def __init__(self, timeout):
        self.timeout = timeout

    def __call__(self, f):
        f._test_timeout = self.timeout
        return f


def get_test_timeout(o, timeout):
    val = getattr(o, '_test_timeout', 0)
    return max(val, timeout)


class ActorTestMixin:
    '''A mixin for :class:`~unittest.TestCase`.

    Useful for classes testing spawning of actors.
    Make sure this is the first class you derive from, before the
    :class:`~unittest.TestCase`, so that the tearDown method is overwritten.

    .. attribute:: concurrency

        The concurrency model used to spawn actors via the :meth:`spawn`
        method.
    '''
    concurrency = 'thread'

    @property
    def all_spawned(self):
        if not hasattr(self, '_spawned'):
            self._spawned = []
        return self._spawned

    async def spawn_actor(self, concurrency=None, **kwargs):
        '''Spawn a new actor and perform some tests
        '''
        concurrency = concurrency or self.concurrency
        ad = spawn(concurrency=concurrency, **kwargs)
        self.assertTrue(ad.aid)
        self.assertIsInstance(ad, asyncio.Future)
        proxy = await ad
        self.all_spawned.append(proxy)
        self.assertEqual(proxy.aid, ad.aid)
        self.assertEqual(proxy.proxy, proxy)
        self.assertTrue(proxy.cfg)
        return proxy

    def stop_actors(self, *args):
        all = args or self.all_spawned
        return as_gather(*[send(a, 'stop') for a in all])

    def tearDown(self):
        return self.stop_actors()


class check_server:

    def __init__(self, name):
        self.name = name

    async def __call__(self):
        cfg = get_actor().cfg
        addr = cfg.get('%s_server' % self.name)
        if addr:
            if ('%s://' % self.name) not in addr:
                addr = '%s://%s' % (self.name, addr)
            try:
                sync_store = create_store(addr)
            except ImproperlyConfigured:
                return False
            try:
                await sync_store.ping()
                return True
            except Exception:
                return False
        else:
            return False


def dont_run_with_thread(obj):
    '''Decorator for disabling process based test cases when the test suite
    runs in threading, rather than processing, mode.
    '''
    actor = get_actor()
    if actor:
        d = unittest.skipUnless(actor.cfg.concurrency == 'process',
                                'Run only when concurrency is process')
        return d(obj)
    else:
        return obj


async def run_test_server(cls, server, name=None, bind=None, **kwargs):
    """Utility function to sown a socket server and wait for
    workers to be up
    """
    kwargs['parse_console'] = False
    kwargs['name'] = name or cls.__name__.lower()
    kwargs['concurrency'] = getattr(cls, 'concurrency', 'process')
    kwargs['bind'] = bind or '127.0.0.1:0'
    s = server(**kwargs)
    cls.app_cfg = await send('arbiter', 'run', s)
    await asyncio.sleep(0.5)
    cls.uri = 'http://{0}:{1}'.format(*cls.app_cfg.addresses[0])
