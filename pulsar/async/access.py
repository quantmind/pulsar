import os
import threading
import logging
from collections import OrderedDict
from threading import current_thread
from multiprocessing import current_process

import asyncio
from asyncio.futures import _PENDING, _CANCELLED, _FINISHED
from asyncio.base_events import BaseEventLoop, _StopError

from pulsar.utils.config import Global

__all__ = ['get_request_loop',
           'get_event_loop',
           'new_event_loop',
           'asyncio',
           'get_actor',
           'is_mainthread',
           'process_data',
           'thread_data',
           'logger',
           'get_logger',
           'NOTHING',
           'SELECTORS']


LOGGER = logging.getLogger('pulsar')
NOTHING = object()
SELECTORS = OrderedDict()

for selector in ('Epoll', 'Kqueue', 'Poll', 'Select'):
    name = '%sSelector' % selector
    selector_class = getattr(asyncio.selectors, name, None)
    if selector_class:
        SELECTORS[selector.lower()] = selector_class


if os.environ.get('BUILDING-PULSAR-DOCS') == 'yes':     # pragma nocover
    default_selector = 'epoll on linux, kqueue on mac, select on windows'
else:
    default_selector = tuple(SELECTORS)[0]


class PollerSetting(Global):
    name = "selector"
    flags = ["--io"]
    choices = tuple(SELECTORS)
    default = default_selector
    desc = """\
        Specify the default selector used for I/O event polling.

        The default value is the best possible for the system running the
        application.
        """

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


def get_logger(default=None):
    return getattr(get_request_loop(), 'logger', default or LOGGER)


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
