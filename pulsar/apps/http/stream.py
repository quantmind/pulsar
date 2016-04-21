import asyncio


class StreamConsumedError(Exception):
    """Same name as request error"""
    pass


class HttpStream:
    """An asynchronous streaming body for an HTTP response
    """
    def __init__(self, response):
        self._response = response
        self._streamed = False
        self._queue = asyncio.Queue()

    def __repr__(self):
        return repr(self._response)
    __str__ = __repr__

    @property
    def done(self):
        """Check if the stream is finished
        """
        return self._response.on_finished.fired()

    async def read(self, n=None):
        """Read all content
        """
        if self._streamed:
            return b''
        buffer = []
        async for body in self:
            buffer.append(body)
        return b''.join(buffer)

    def close(self):
        pass

    def __iter__(self):
        return _start_iter(self)

    def __next__(self):
        if self.done:
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                raise StopIteration
        else:
            return self._queue.get()

    async def __aiter__(self):
        return _start_iter(self)

    async def __anext__(self):
        if self.done:
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                raise StopAsyncIteration
        else:
            return await self._queue.get()

    def __call__(self, response, exc=None, **kw):
        if self._streamed and response.parser.is_headers_complete():
            assert response is self._response
            self._queue.put_nowait(response.recv_body())


def _start_iter(self):
    if self._streamed:
        raise StreamConsumedError
    self._streamed = True
    self(self._response)
    return self
