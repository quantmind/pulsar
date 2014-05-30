'''At the moment this module is just an Hack to get pulsar
imported in the google appengine. It may become better in the future.
'''
from google.appengine.ext import ndb
from google.appengine.ext.ndb import eventloop

get_event_loop = eventloop.get_event_loop
selectors = None
ConnectionRefusedError = None


class Future(ndb.Future):

    def __init__(self, loop=None):
        super(Future, self).__init__()
        self._loop = loop

    def result(self):
        return self.get_result()


Task = Future


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
