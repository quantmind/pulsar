import asyncio
from pulsar import isfuture


class HttpStream:
    '''A streaming body for an HTTP response
    '''
    def __init__(self, response):
        self._response = response
        self._queue = asyncio.Queue()

    def __repr__(self):
        return repr(self._response)
    __str__ = __repr__

    @property
    def done(self):
        return self._response.on_finished.fired()

    def read(self, n=None):
        '''Read all content'''
        buffer = []
        for body in self:
            if isfuture(body):
                body = yield from body
            buffer.append(body)
        return b''.join(buffer)

    def __content(self):
        '''Read and store content
        '''
        if self._response._content is None:
            self._response._content = yield from self.read()
        return self._response.content

    def close(self):
        pass

    def __iter__(self):
        '''Iterator over bytes or Futures resulting in bytes
        '''
        while True:
            if self.done:
                try:
                    yield self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            else:
                yield self._queue.get()

    def __call__(self, response, exc=None, **kw):
        body = response.recv_body()
        self._queue.put_nowait(body)
