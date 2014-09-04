'''At the moment this module is just an Hack to get pulsar
imported in the google appengine. It may become better in the future.
'''
import time

from google.appengine.ext import ndb
from google.appengine.ext.ndb import eventloop

from pulsar.utils.pep import identity

get_event_loop = eventloop.get_event_loop
selectors = None
ConnectionRefusedError = None
ConnectionResetError = None
From = identity
Return = identity
coroutine = identity
iscoroutinefunction = lambda c: False


class Future(ndb.Future):

    def __init__(self, loop=None):
        super(Future, self).__init__()
        self._loop = loop

    def result(self):
        return self.get_result()


Task = Future
_FUTURE_CLASSES = (Future,)


class CancelledError(RuntimeError):
    pass


TimeoutError = CancelledError
InvalidStateError = CancelledError


class _StopError(BaseException):
    pass


class QueueFull(Exception):
    pass


class AbstractEventLoop(object):
    pass


class BaseEventLoop(AbstractEventLoop):
    pass


class Protocol(object):
    pass


class DatagramProtocol(object):
    pass


class Queue(object):

    def __init__(self, **kw):
        raise NotImplementedError


def reraise(tp, value, tb=None):
    raise value


def async(core_or_future, loop=None):
    raise TypeError


def sleep(interval, result=None, loop=None):
    time.sleep(interval)
    return result


def iscoroutine(value):
    return False
