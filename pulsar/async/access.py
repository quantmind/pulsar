import os
import sys
import threading
import logging
from functools import wraps
from collections import OrderedDict
from threading import current_thread

from pulsar.utils.config import Global
from pulsar.utils.pep import ispy3k
from pulsar.utils.system import platform, current_process

__all__ = ['get_event_loop',
           'new_event_loop',
           'asyncio',
           'get_actor',
           'isfuture',
           'is_mainthread',
           'process_data',
           'thread_data',
           'logger',
           'NOTHING',
           'SELECTORS',
           'appengine',
           'Future',
           'ConnectionRefusedError',
           'ConnectionResetError',
           'reraise',
           'From',
           'async',
           'sleep',
           'iscoroutine',
           'get_io_loop',
           'CANCELLED_ERRORS']

# Dance between different versions. So boring!
appengine = False

if platform.is_appengine:   # pragma    nocover
    from . import appengine as asyncio
    trollius = asyncio
    _PENDING = 'PENDING'
    _CANCELLED = 'CANCELLED'
    _FINISHED = 'FINISHED'
    _FUTURE_CLASSES = asyncio._FUTURE_CLASSES
    _EVENT_LOOP_CLASSES = ()
    BaseEventLoop = asyncio.BaseEventLoop
    _StopError = asyncio._StopError
    appengine = True
    reraise = asyncio.reraise
    CANCELLED_ERRORS = ()

else:

    # Set the debug flags before importing asyncio
    if '--debug' in sys.argv:   # pragma    nocover
        os.environ['PYTHONASYNCIODEBUG'] = 'debug'
        os.environ['TROLLIUSDEBUG'] = 'debug'

    import trollius

    try:
        import asyncio
    except ImportError:     # pragma    nocover
        asyncio = trollius

    from trollius.futures import (_PENDING, _CANCELLED, _FINISHED,
                                  _FUTURE_CLASSES)
    from trollius.base_events import BaseEventLoop, _StopError
    from trollius import selectors, events
    from trollius.py33_exceptions import reraise

    _EVENT_LOOP_CLASSES = (asyncio.AbstractEventLoop,
                           trollius.AbstractEventLoop)
    CANCELLED_ERRORS = (asyncio.CancelledError, trollius.CancelledError)

Future = trollius.Future
From = trollius.From
Return = trollius.Return
async = trollius.async
sleep = trollius.sleep
iscoroutine = trollius.iscoroutine
iscoroutinefunction = trollius.iscoroutinefunction
ConnectionRefusedError = trollius.ConnectionRefusedError
ConnectionResetError = trollius.ConnectionResetError

isfuture = lambda x: isinstance(x, _FUTURE_CLASSES)


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
elif SELECTORS:
    default_selector = tuple(SELECTORS)[0]
else:
    default_selector = None


if default_selector:
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


def get_io_loop(loop=None):
    if not isinstance(loop, _EVENT_LOOP_CLASSES):
        actor = get_actor()
        return actor._loop if actor else new_event_loop()
    return loop


def new_event_loop(**kwargs):
    '''Obtain a new event loop.'''
    loop = asyncio.new_event_loop()
    if hasattr(loop, 'setup_loop'):
        loop.setup_loop(**kwargs)
    return loop


def is_mainthread(thread=None):
    '''Check if thread is the main thread.

    If ``thread`` is not supplied check the current thread
    '''
    thread = thread if thread is not None else current_thread()
    return isinstance(thread, threading._MainThread)


def logger(loop=None, logger=None):
    return getattr(loop or get_event_loop(), 'logger', LOGGER)


def process_data(name=None):
    '''Fetch the current process local data dictionary.

    If ``name`` is not ``None`` it returns the value at``name``,
    otherwise it return the process data dictionary
    '''
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
