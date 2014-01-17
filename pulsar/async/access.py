import threading
import logging
from threading import current_thread
from multiprocessing import current_process

try:
    import asyncio
    from asyncio.futures import _PENDING, _CANCELLED, _FINISHED
except ImportError:  # pragma    nocover
    from .fallbacks import asyncio
    _PENDING = asyncio._PENDING
    _CANCELLED = asyncio._CANCELLED
    _FINISHED = asyncio._FINISHED


__all__ = ['get_request_loop',
           'get_event_loop',
           'new_event_loop',
           'asyncio',
           'get_actor',
           'is_mainthread',
           'process_data',
           'thread_data',
           'logger',
           'NOTHING',
           'AsyncObject']


LOGGER = logging.getLogger('pulsar')
NOTHING = object()


get_event_loop = asyncio.get_event_loop


def new_event_loop(**kwargs):
    '''Obtain a new event loop.'''
    loop = asyncio.new_event_loop()
    if hasattr(loop, 'setup_loop'):
        loop.setup_loop(**kwargs)
    return loop


def is_mainthread(thread=None):
    '''Check if thread is the main thread. If ``thread`` is not supplied check
the current thread.'''
    thread = thread if thread is not None else current_thread()
    return isinstance(thread, threading._MainThread)


def get_request_loop():
    return asyncio.get_event_loop_policy().get_request_loop()


def logger(loop=None):
    return getattr(loop or get_request_loop(), 'logger', LOGGER)


def process_data(name=None):
    '''Fetch the current process local data dictionary. If *name* is not
``None`` it returns the value at *name*, otherwise it return the process data
dictionary.'''
    ct = current_process()
    if not hasattr(ct, '_pulsar_local'):
        ct._pulsar_local = {}
    loc = ct._pulsar_local
    return loc.get(name) if name else loc


def thread_data(name, value=NOTHING, ct=None):
    '''Set or retrieve an attribute ``name`` from thread ``ct``.

    If ``ct`` is not given used the current thread. If ``value``
    is None, it will get the value otherwise it will set the value.
    '''
    ct = ct or current_thread()
    if is_mainthread(ct):
        loc = process_data()
    elif not hasattr(ct, '_pulsar_local'):
        ct._pulsar_local = loc = {}
    else:
        loc = ct._pulsar_local
    if value is not NOTHING:
        if name in loc:
            if loc[name] is not value:
                raise RuntimeError(
                    '%s is already available on this thread' % name)
        else:
            loc[name] = value
    return loc.get(name)


get_actor = lambda: thread_data('actor')
set_actor = lambda actor: thread_data('actor', actor)


class AsyncObject(object):
    '''Interface for :ref:`async objects <async-object>`

    .. attribute:: _loop

        The event loop associated with this object
    '''
    _loop = None

    @property
    def logger(self):
        '''The logger for this object
        '''
        return getattr(self._loop, 'logger', LOGGER)
