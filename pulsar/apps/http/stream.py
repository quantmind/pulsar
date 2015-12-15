

class HttpStream:
    '''A streaming body for an HTTP response
    '''
    def __init__(self, response):
        self._response = response

    def __repr__(self):
        return repr(self._response)
    __str__ = __repr__

    def read(self):
        '''Read all content'''
        yield from self._response.on_finished
        return self._response.recv_body()
