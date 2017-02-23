from pulsar.api import Http404
from pulsar.utils.log import LocalMixin, local_method

from .utils import handle_wsgi_error


class WsgiHandler:
    '''An handler for applications conforming to python WSGI_.

    .. attribute:: middleware

        List of :ref:`asynchronous WSGI middleware <wsgi-middleware>` callables
        which accept ``environ`` and ``start_response`` as arguments.
        The order matter, since the response returned by the callable
        is the non ``None`` value returned by a middleware.

    .. attribute:: response_middleware

        List of functions of the form::

            def ..(environ, response):
                ...

        where ``response`` is a :ref:`WsgiResponse <wsgi-response>`.
        Pulsar contains some
        :ref:`response middlewares <wsgi-response-middleware>`.

    '''
    def __init__(self, middleware=None, response_middleware=None, async=True):
        if middleware:
            middleware = list(middleware)
        self.middleware = middleware or []
        self.response_middleware = response_middleware or []
        self._async = async

    def __call__(self, environ, start_response):
        c = self._async_call if self._async else self._sync_call
        return c(environ, start_response)

    async def _async_call(self, environ, start_response):
        response = None
        try:
            for middleware in self.middleware:
                response = middleware(environ, start_response)
                if response:
                    try:
                        response = await response
                    except TypeError:
                        pass
                if response is not None:
                    break
            if response is None:
                raise Http404

        except Exception as exc:
            response = handle_wsgi_error(environ, exc)

        if not getattr(response, '__wsgi_started__', True):
            for middleware in self.response_middleware:
                result = middleware(environ, response)
                if result:
                    try:
                        response = await result
                    except TypeError:
                        response = result

            response.start(environ, start_response)
        return response

    def _sync_call(self, environ, start_response):
        response = None
        try:
            for middleware in self.middleware:
                response = middleware(environ, start_response)
                if response is not None:
                    break
            if response is None:
                raise Http404

        except Exception as exc:
            response = handle_wsgi_error(environ, exc)

        if not getattr(response, '__wsgi_started__', True):
            for middleware in self.response_middleware:
                response = middleware(environ, response) or response
            response.start(environ, start_response)
        return response


class LazyWsgi(LocalMixin):
    '''A :ref:`wsgi handler <wsgi-handlers>` which loads the actual
    handler the first time it is called.

    Subclasses must implement the :meth:`setup` method.
    Useful when working in multiprocessing mode when the application
    handler must be a ``picklable`` instance. This handler can rebuild
    its wsgi :attr:`handler` every time is pickled and un-pickled without
    causing serialisation issues.
    '''
    def __call__(self, environ, start_response):
        if not hasattr(self, '_local'):
            return self.handler(environ)(environ, start_response)
        return self._local.handler(environ, start_response)

    @local_method
    def handler(self, environ=None):
        '''The :ref:`wsgi application handler <wsgi-handlers>` which
        is loaded via the :meth:`setup` method, once only,
        when first accessed.
        '''
        return self.setup(environ)

    def setup(self, environ=None):
        '''The setup function for this :class:`LazyWsgi`.

        Called once only the first time this application handler is invoked.
        This **must** be implemented by subclasses and **must** return a
        :ref:`wsgi application handler <wsgi-handlers>`.
        '''
        raise NotImplementedError
