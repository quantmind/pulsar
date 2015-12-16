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

    def read(self):
        '''Read all content'''
        buffer = []
        for body in self:
            if isfuture(body):
                body = yield from body
            buffer.append(body)
        return b''.join(buffer)

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
                yield asyncio.async(self.queue.get())

    def __call__(self, response, exc=None, **kw):
        body = response.recv_body()
        if body:
            self._queue.put_nowait(body)
