from .access import create_future


class FlowControl:
    """A protocol mixin for flow control logic.

    This implements the protocol methods :meth:`pause_writing`,
    :meth:`resume_writing`.
    """
    _paused = False
    _write_waiter = None

    def pause_writing(self):
        '''Called by the transport when the buffer goes over the
        high-water mark

        Successive calls to this method will fails unless
        :meth:`resume_writing` is called first.
        '''
        assert not self._paused
        self._paused = True
        self.transport.pause_reading()

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
        self.transport.resume_reading()

    # INTERNAL CALLBACKS
    def _set_flow_limits(self, _, exc=None):
        if not exc:
            self.transport.set_write_buffer_limits(
                self._low_limit, self._high_limit
            )

    def _wakeup_waiter(self, _, exc=None):
        # Wake up the writer if currently paused.
        if not self._paused:
            return
        self.resume_writing(exc=exc)

    def _make_write_waiter(self):
        # callback for the after_write event
        if self._paused:
            waiter = self._write_waiter
            assert waiter is None or waiter.cancelled()
            waiter = create_future(self._loop)
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
            self.event('connection_made').bind(self._add_timeout)
            self.event('connection_lost').bind(self._cancel_timeout)
        self._timeout = timeout or 0
        self._add_timeout(None)

    # INTERNALS
    def _timed_out(self):
        if self.last_change:
            gap = self._loop.time() - self.last_change
            if gap < self._timeout:
                self._timeout_handler = None
                return self._add_timeout(None, timeout=self._timeout-gap)
        self.close()
        self.logger.debug('Closed idle %s.', self)

    def _add_timeout(self, _, exc=None, timeout=None):
        if not self.closed:
            self._cancel_timeout(_, exc=exc)
            timeout = timeout or self._timeout
            if timeout and not exc:
                self._timeout_handler = self._loop.call_later(
                    timeout, self._timed_out
                )

    def _cancel_timeout(self, _, exc=None, **kw):
        if self._timeout_handler:
            self._timeout_handler.cancel()
            self._timeout_handler = None
