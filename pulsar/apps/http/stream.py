import asyncio

from pulsar import isfuture, ensure_future


class StreamConsumedError(Exception):
    '''Same name as request error'''
    pass


class HttpStream:
    '''A streaming body for an HTTP response
    '''
    def __init__(self, response):
        self._streamed = False
        self._response = response
        self._queue = asyncio.Queue()

    def __repr__(self):
        return repr(self._response)
    __str__ = __repr__

    @property
    def done(self):
        return self._response.on_finished.fired()

    @asyncio.coroutine
    def read(self, n=None):
        '''Read all content'''
        if self._streamed:
            return b''
        buffer = []
        for body in self:
            if isfuture(body):
                body = yield from body
            buffer.append(body)
        return b''.join(buffer)

    def close(self):
        pass

    def __iter__(self):
        '''Iterator over bytes or Futures resulting in bytes
        '''
        if self._streamed:
            raise StreamConsumedError
        self._streamed = True
        self(self._response)
        while True:
            if self.done:
                try:
                    yield self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            else:
                yield ensure_future(self._queue.get())

    def __call__(self, response, exc=None, **kw):
        if self._streamed and response.parser.is_headers_complete():
            assert response is self._response
            self._queue.put_nowait(response.recv_body())
