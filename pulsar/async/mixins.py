import time

from asyncio import Queue, CancelledError


DEFAULT_LIMIT = 2**16


class FlowControl:
    """A protocol mixin for flow control logic.

    This implements the protocol methods :meth:`pause_writing`,
    :meth:`resume_writing`.
    """
    _b_limit = 2*DEFAULT_LIMIT
    _paused = False
    _buffer_size = 0
    _waiter = None

    def write(self, data):
        """Write ``data`` into the wire.

        Returns an empty tuple or a :class:`~asyncio.Future` if this
        protocol has paused writing.
        """
        if self.closed:
            raise ConnectionResetError(
                'Transport closed - cannot write on %s' % self
            )
        else:
            t = self.transport
            if self._paused or self._buffer:
                self._buffer.appendleft(data)
                self._buffer_size += len(data)
                self._write_from_buffer()
                if self._buffer_size > 2 * self._b_limit:
                    if self._waiter and not self._waiter.cancelled():
                        self.logger.warning(
                            '%s buffer size is %d: limit is %d ',
                            self._buffer_size, self._b_limit
                        )
                    else:
                        t.pause_reading()
                        self._waiter = self._loop.create_future()
            else:
                t.write(data)
            self.changed()
            return self._waiter

    def pause_writing(self):
        '''Called by the transport when the buffer goes over the
        high-water mark

        Successive calls to this method will fails unless
        :meth:`resume_writing` is called first.
        '''
        assert not self._paused
        self._paused = True

    def resume_writing(self, exc=None):
        '''Resume writing.

        Successive calls to this method will fails unless
        :meth:`pause_writing` is called first.
        '''
        assert self._paused
        self._paused = False
        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.done():
                if exc is None:
                    waiter.set_result(None)
                else:
                    waiter.set_exception(exc)
            self.transport.resume_reading()
        self._write_from_buffer()

    # INTERNAL CALLBACKS
    def _write_from_buffer(self):
        t = self.transport
        if not t:
            return
        while not self._paused and self._buffer:
            if t.is_closing():
                self.producer.logger.debug(
                    'Transport closed - cannot write on %s', self
                )
                break
            data = self._buffer.pop()
            self._buffer_size -= len(data)
            self.transport.write(data)

    def _set_flow_limits(self, _, exc=None):
        if not exc:
            self.transport.set_write_buffer_limits(high=self._limit)

    def _wakeup_waiter(self, _, exc=None):
        # Wake up the writer if currently paused.
        if not self._paused:
            return
        self.resume_writing(exc=exc)


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
            gap = time.time() - self.last_change
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


class Pipeline:
    """Pipeline protocol consumers once reading is finished

    This mixin can be used by TCP connections to pipeline response writing
    """
    _pipeline = None

    def pipeline(self, consumer):
        """Add a consumer to the pipeline
        """
        if self._pipeline is None:
            self._pipeline = ResponsePipeline(self)
            self.event('connection_lost').bind(self._close_pipeline)
        self._pipeline.put(consumer)

    def close_pipeline(self):
        if self._pipeline:
            p, self._pipeline = self._pipeline, None
            return p.close()

    def _close_pipeline(self, _, **kw):
        self.close_pipeline()


class ResponsePipeline:
    """Maintains a queue of responses to send back to the client
    """
    __slots__ = ('connection', 'queue', 'debug', 'worker', 'put')

    def __init__(self, connection):
        self.connection = connection
        self.queue = Queue(loop=connection._loop)
        self.debug = connection._loop.get_debug()
        self.worker = self.queue._loop.create_task(self._process())
        self.put = self.queue.put_nowait

    async def _process(self):
        while True:
            try:
                consumer = await self.queue.get()
                if self.debug:
                    self.connection.producer.logger.debug(
                        'Connection pipeline process %s', consumer
                    )
                await consumer.write_response()
            except (CancelledError, GeneratorExit, RuntimeError):
                break
            except Exception:
                self.connection.producer.logger.exception(
                    'Critical exception in %s response pipeline',
                    self.connection
                )
                self.connection.close()
                break
        # help gc
        self.connection = None
        self.queue = None
        self.worker = None
        self.put = None

    def close(self):
        self.worker.cancel()
        return self.worker
