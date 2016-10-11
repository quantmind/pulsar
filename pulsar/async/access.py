import os
import threading
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

from collections import OrderedDict
from threading import current_thread

from asyncio import Future

from pulsar.utils.config import Global
from pulsar.utils.system import current_process

from asyncio import ensure_future
from inspect import isawaitable


__all__ = ['get_event_loop',
           'new_event_loop',
           'get_actor',
           'cfg',
           'cfg_value',
           'isfuture',
           'create_future',
           'is_mainthread',
           'process_data',
           'thread_data',
           'logger',
           'NOTHING',
           'EVENT_LOOPS',
           'Future',
           'reraise',
           'isawaitable',
           'ensure_future',
           'CANCELLED_ERRORS']


_EVENT_LOOP_CLASSES = (asyncio.AbstractEventLoop,)
CANCELLED_ERRORS = (asyncio.CancelledError,)


def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


def isfuture(x):
    return isinstance(x, Future)


def create_future(loop=None):
    loop = loop or get_event_loop()
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


LOGGER = logging.getLogger('pulsar')
NOTHING = object()
EVENT_LOOPS = OrderedDict()

DefaultLoopClass = asyncio.get_event_loop_policy()._loop_factory


def make_loop_factory(selector):

    def loop_factory():
        return DefaultLoopClass(selector())

    return loop_factory


for selector in ('Epoll', 'Kqueue', 'Poll', 'Select'):
    name = '%sSelector' % selector
    selector_class = getattr(asyncio.selectors, name, None)
    if selector_class:
        EVENT_LOOPS[selector.lower()] = make_loop_factory(selector_class)


try:    # add uvloop if available
    import uvloop
    EVENT_LOOPS['uv'] = uvloop.Loop
except Exception:     # pragma    nocover
    pass


if os.environ.get('BUILDING-PULSAR-DOCS') == 'yes':     # pragma nocover
    default_loop = (
        'uvloop if available, epoll on linux, '
        'kqueue on mac, select on windows'
    )
elif EVENT_LOOPS:
    default_loop = tuple(EVENT_LOOPS)[0]
else:
    default_loop = None


if default_loop:
    class EventLoopSetting(Global):
        name = "event_loop"
        flags = ["--io"]
        choices = tuple(EVENT_LOOPS)
        default = default_loop
        desc = """\
            Specify the event loop used for I/O event polling.

            The default value is the best possible for the system running the
            application.
            """

get_event_loop = asyncio.get_event_loop
new_event_loop = asyncio.new_event_loop


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


def get_actor():
    return thread_data('actor')


def set_actor(actor):
    return thread_data('actor', actor)


def cfg():
    actor = get_actor()
    if actor:
        return actor.cfg


def cfg_value(setting, value=None):
    if value is None:
        actor = get_actor()
        if actor:
            return actor.cfg.get(setting)
    return value


class EventLoopPolicy(asyncio.DefaultEventLoopPolicy):

    def __init__(self, name, workers, debug):
        super().__init__()
        self.name = name
        self.workers = workers
        self.debug = debug

    @property
    def _local(self):
        l = getattr(current_process(), '_event_loop_policy', None)
        if l is None:
            self._local = l = self._Local()
        return l

    @_local.setter
    def _local(self, v):
        current_process()._event_loop_policy = v

    def _loop_factory(self):
        loop = EVENT_LOOPS[self.name]()
        loop.set_default_executor(ThreadPoolExecutor(self.workers))
        if self.debug:
            loop.set_debug(True)
        return loop
