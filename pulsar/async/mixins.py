from .futures import Future


class FlowControl:
    """A protocol mixin for flow control logic.

    This implements the protocol methods :meth:`pause_writing`,
    :meth:`resume_writing`.
    """
    _paused = False
    _write_waiter = None

    def __init__(self, low_limit=None, high_limit=None, **kw):
        self._low_limit = low_limit
        self._high_limit = high_limit
        self.bind_event('connection_made', self._set_flow_limits)
        self.bind_event('connection_lost', self._wakeup_waiter)
        self.bind_event('after_write', self._make_write_waiter)

    def pause_writing(self):
        '''Called by the transport when the buffer goes over the
        high-water mark

        Successive calls to this method will fails unless
        :meth:`resume_writing` is called first.
        '''
        assert not self._paused
        self._paused = True
        self._transport.pause_reading()

    def resume_writing(self, exc=None):
        '''Resume writing.

        Successive calls to this method will fails unless
        :meth:`pause_writing` is called first.
        '''
        assert self._paused
        self._paused = False
        waiter = self._write_waiter
        if waiter is not None:
            self._write_waiter = None
            if not waiter.done():
                if exc is None:
                    waiter.set_result(None)
                else:
                    waiter.set_exception(exc)
        self._transport.resume_reading()

    # INTERNAL CALLBACKS
    def _set_flow_limits(self, _, exc=None):
        if not exc:
            self._transport.set_write_buffer_limits(self._low_limit,
                                                    self._high_limit)

    def _wakeup_waiter(self, _, exc=None):
        # Wake up the writer if currently paused.
        if not self._paused:
            return
        self.resume_writing(exc=exc)

    def _make_write_waiter(self, _, exc=None):
        # callback for the after_write event
        if self._paused:
            waiter = self._write_waiter
            assert waiter is None or waiter.cancelled()
            waiter = Future(loop=self._loop)
            self.logger.debug('Waiting for write buffer to drain')
            self._write_waiter = waiter


class Timeout:
    '''Adds a timeout for idle connections to protocols
    '''
    _timeout = None
    _timeout_handler = None

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, timeout):
        '''Set a new :attr:`timeout` for this protocol
        '''
        if self._timeout is None:
            self.bind_event('connection_made', self._add_timeout)
            self.bind_event('connection_lost', self._cancel_timeout)
            self.bind_event('before_write', self._cancel_timeout)
            self.bind_event('after_write', self._add_timeout)
            self.bind_event('data_received', self._cancel_timeout)
            self.bind_event('data_processed', self._add_timeout)
        self._timeout = timeout or 0
        self._add_timeout(None)

    # INTERNALS
    def _timed_out(self):
        self.close()
        self.logger.debug('Closed idle %s.', self)

    def _add_timeout(self, _, exc=None, **kw):
        if not self.closed:
            self._cancel_timeout(_, exc=exc)
            if self._timeout and not exc:
                self._timeout_handler = self._loop.call_later(self._timeout,
                                                              self._timed_out)

    def _cancel_timeout(self, _, exc=None, **kw):
        if self._timeout_handler:
            self._timeout_handler.cancel()
            self._timeout_handler = None
