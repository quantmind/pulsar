'''Not used yet
'''
from pulsar import Future


class Throttling:
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
