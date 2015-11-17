from . import wait


class GreenWSGI:
    '''Wraps a WSGI application to be executed on a :class:`.GreenPool`
    '''
    def __init__(self, wsgi, pool):
        self.wsgi = wsgi
        self.pool = pool

    def __call__(self, environ, start_response):
        return self.pool.submit(self._green_handler, environ, start_response)

    def _green_handler(self, environ, start_response):
        if environ['wsgi.input']:
            environ['wsgi.input'] = GreenStream(environ['wsgi.input'])
        return wait(self.wsgi(environ, start_response))


class GreenStream:
    __slots__ = ('stream',)

    def __init__(self, stream):
        self.stream = stream

    def __getattr__(self, name):
        value = getattr(self.stream, name)
        if getattr(value, '__self__', None) is self.stream:
            return _green(value)
        return value


class _green:
    __slots__ = ('value',)

    def __init__(self, value):
        self.value = value

    def __getattr__(self, name):
        return getattr(self.value, name)

    def __call__(self, *args, **kwargs):
        return wait(self.value(*args, **kwargs))
