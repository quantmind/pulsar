from pulsar import Http404
from pulsar.apps.wsgi import handle_wsgi_error, WsgiResponse

from .utils import wait


class GreenWSGI:
    '''Wraps a WSGI application to be executed on a :class:`.GreenPool`
    '''
    def __init__(self, middleware, pool, response_middleware=None):
        if not isinstance(middleware, (list, tuple)):
            middleware = [middleware]
        self.middleware = list(middleware)
        self.response_middleware = response_middleware or []
        self.pool = pool

    def __call__(self, environ, start_response):
        if self.pool.in_green_worker:
            return self._call(environ, start_response)
        else:
            return self.pool.submit(self._call, environ, start_response)

    def _call(self, environ, start_response):
        wsgi_input = environ['wsgi.input']
        if wsgi_input and not isinstance(wsgi_input, GreenStream):
            environ['wsgi.input'] = GreenStream(wsgi_input)
        response = None
        try:
            for middleware in self.middleware:
                response = wait(middleware(environ, start_response))
                if response is not None:
                    break
            if response is None:
                raise Http404

        except Exception as exc:
            response = wait(handle_wsgi_error(environ, exc))

        if isinstance(response, WsgiResponse) and not response.started:
            for middleware in self.response_middleware:
                response = wait(middleware(environ, response)) or response
            response.start(start_response)
        return response


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
