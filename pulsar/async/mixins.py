from .futures import Future, asyncio


class ProtocolWrapper(object):

    def __init__(self, protocol):
        self.protocol = protocol

    def __repr__(self):
        return self.protocol.__repr__()
    __str__ = __repr__

    def __getattr__(self, name):
        return getattr(self.protocol, name)

    def write(self, data):
        return self.protocol.write(data)

    def gather(self, a, b):
        if a:
            return asyncio.gather(a, b) if b else a
        else:
            return b


class FlowControl(ProtocolWrapper):
    """Reusable flow control logic for StreamWriter.drain().

    This implements the protocol methods pause_writing(),
    resume_reading() and connection_lost().  If the subclass overrides
    these it must call the super methods.

    StreamWriter.drain() must check for error conditions and then call
    _make_drain_waiter(), which will return either () or a Future
    depending on the paused state.
    """
    _paused = False
    _drain_waiter = None

    def __init__(self, protocol, read_limit=None, write_limit=None):
        self.protocol = protocol
        self.bind_event('connection_lost', self._wakeup_waiter)

    def write(self, data):
        result = self.protocol.write(data)
        waiter = self._make_drain_waiter()
        return self.gather(result, waiter)

    def pause_writing(self):
        assert not self._paused
        self._paused = True

    def resume_writing(self):
        assert self._paused
        self._paused = False
        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    def _wakeup_waiter(self, _, exc=None):
        # Wake up the writer if currently paused.
        if not self._paused:
            return
        waiter = self._drain_waiter
        if waiter is None:
            return
        self._drain_waiter = None
        if waiter.done():
            return
        if exc is None:
            waiter.set_result(None)
        else:
            waiter.set_exception(exc)

    def _make_drain_waiter(self):
        if not self._paused:
            return ()
        waiter = self._drain_waiter
        assert waiter is None or waiter.cancelled()
        waiter = Future(loop=self._loop)
        self.protocol.logger.debug('Waiting for write buffer to drain')
        self._drain_waiter = waiter
        return waiter


class Timeout(ProtocolWrapper):

    def __init__(self, protocol, timeout):
        self.protocol = protocol
        self.timeout = timeout
        self._timeout = None
        protocol.bind_event('connection_made', self._add_timeout)
        protocol.bind_event('connection_lost', self._cancel_timeout)

    def write(self, data):
        return self._with_timeout(self.protocol.write, data)

    def data_received(self, data):
        self._with_timeout(self.protocol.data_received, data)

    def set_timeout(self, timeout):
        '''Set a new :attr:`timeout` for this protocol
        '''
        self._cancel_timeout()
        self.timeout = timeout
        self._add_timeout()

    def info(self):
        info = self.protocol.info()
        info['connection']['timeout'] = self.timeout
        return info

    # INTERNALS
    def _with_timeout(self, method, *args):
        self._cancel_timeout()
        result = method(*args)
        if isinstance(result, Future):
            result.add_done_callback(lambda f: self._add_timeout())
        else:
            self._add_timeout()
        return result

    def _timed_out(self):
        self.protocol.close()
        self.protocol.logger.debug('Closed idle %s.', self.protocol)

    def _add_timeout(self, *args, **kw):
        p = self.protocol
        if not p.closed:
            self._cancel_timeout()
            if self.timeout:
                self._timeout = p._loop.call_later(self.timeout,
                                                   self._timed_out)

    def _cancel_timeout(self, *args, **kw):
        if self._timeout:
            self._timeout.cancel()
            self._timeout = None


class Throttling(ProtocolWrapper):
    _types = ('read', 'write')

    def __init__(self, protocol, read_limit=None, write_limit=None):
        self.protocol = protocol
        self.limits = (read_limit, write_limit)
        self.this_second = [0, 0]
        self._checks = [None, None]
        self._throttle = [None, None]

    def data_received(self, data):
        '''Receive new data and pause the receiving end of the transport
        if the raed limit per second is surpassed
        '''
        if not self._checks[0]:
            self._check_limit(0)
        self._register(data, 0)
        self.protocol.data_received(data)

    def write(self, data):
        '''Write ``data`` and return either and empty tuple or
        an :class:`~asyncio.Future` called back once the write limit
        per second is not surpassed.
        '''
        if not self._checks[1]:
            self._check_limit(1)
        self._register(data, 1)
        result = self.protocol.write(data)
        waiter = self._throttle[1]
        return self.gather(result, waiter)

    # INTERNALS
    def _register(self, data, rw):
        self.this_second[rw] += len(data)

    def _check_limit(self, rw):
        loop = self.protocol._loop
        limit = self.limits[rw]
        if limit and self.this_second[rw] > limit:
            self._thorttle(rw)
            next = (float(self.this_second[rw]) / limit) - 1.0
            loop.call_later(next, self._un_throttle, rw)
        self.this_second[rw] = 0
        self._checks[rw] = loop.call_later(1, self._check_limit, rw)

    def _throttle(self, rw):
        self.logger.debug('Throttling %s', self._types[rw])
        if rw:
            assert not self._throttle[rw]
            self._throttle[rw] = Future(self.protocol._loop)
        else:
            self._throttle[rw] = True
            t = self.protocol._transport
            t.pause_reading()

    def _un_throttle(self, rw):
        self.protocol.logger.debug('Un-throttling %s', self._types[rw])
        if rw:
            waiter = self._throttle[rw]
            self._throttle[rw] = None
            if waiter and not waiter.done():
                waiter.set_result(None)
        else:
            t = self.protocol._transport
            if t:
                t.resume_reading()
