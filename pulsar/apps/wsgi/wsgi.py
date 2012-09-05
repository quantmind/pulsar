import os
import json
import textwrap
import logging
from functools import partial

import pulsar
from pulsar import maybe_async, is_async, is_failure, log_failure, NOT_DONE
from pulsar.utils.httpurl import Headers, SimpleCookie, set_cookie, responses,\
                                    has_empty_content, string_type, ispy3k

from .middleware import is_streamed

if ispy3k:
    from functools import reduce

__all__ = ['WsgiHandler',
           'WsgiResponse',
           'WsgiResponseGenerator',
           'wsgi_iterator',
           'handle_http_error']


default_logger = logging.getLogger('pulsar.apps.wsgi')


def wsgi_iterator(gen, encoding=None):
    encoding = encoding or 'utf-8'
    for data in gen:
        data = maybe_async(data)
        while is_async(data):
            yield b''
            data = maybe_async(data)
        if data is NOT_DONE:
            yield b''
        elif data is None:
            continue
        elif is_failure(data):
            log_failure(data)
        else:
            if isinstance(data, bytes):
                yield data
            elif isinstance(data, string_type):
                yield data.encode(encoding)
            else:
                for b in wsgi_iterator(data, encoding):
                    yield b


class WsgiResponseGenerator(object):

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response
        self.middleware = []

    def __iter__(self):
        raise NotImplementedError()

    def start(self, response):
        '''Start the response generator'''
        response.middleware.extend(self.middleware)
        for b in response(self.environ, self.start_response):
            yield b


class WsgiResponse(WsgiResponseGenerator):
    '''A WSGI response wrapper initialized by a WSGI request middleware.
Instances are callable using the standard WSGI call::

    response = WsgiResponse(200)
    response(environ, start_response)

A :class:`WsgiResponse` is an iterable over bytes to send back to the requesting
client.

.. attribute:: status_code

    Integer indicating the HTTP status, (i.e. 200)

.. attribute:: response

    String indicating the HTTP status (i.e. 'OK')

.. attribute:: status

    String indicating the HTTP status code and response (i.e. '200 OK')

.. attribute:: environ

    The dictionary of WSGI environment if passed to the constructor.

'''
    _started = False
    DEFAULT_STATUS_CODE = 200
    def __init__(self, status=None, content=None, response_headers=None,
                 content_type=None, encoding=None, environ=None,
                 start_response=None):
        super(WsgiResponse, self).__init__(environ, start_response)
        self.status_code = status or self.DEFAULT_STATUS_CODE
        self.encoding = encoding
        self.cookies = SimpleCookie()
        self.headers = Headers(response_headers, kind='server')
        self.content = content
        if content_type is not None:
            self.content_type = content_type

    @property
    def started(self):
        return self._started

    @property
    def path(self):
        if self.environ:
            return self.environ.get('PATH_INFO','')

    @property
    def method(self):
        if self.environ:
            return self.environ.get('REQUEST_METHOD')

    @property
    def connection(self):
        if self.environ:
            return self.environ.get('pulsar.connection')

    def _get_content(self):
        return self._content
    def _set_content(self, content):
        if not self._started:
            if content is None:
                # no content, get the default content
                content = self.default_content()
            elif isinstance(content, bytes):
                content = (content,)
            self._content = content
        else:
            raise RuntimeError('Cannot set content. Already iterated')
    content = property(_get_content, _set_content)
    
    def _get_content_type(self):
        return self.headers.get('content-type')
    def _set_content_type(self, typ):
        self.headers['content-type'] = typ
    content_type = property(_get_content_type, _set_content_type)

    def default_content(self):
        '''Called during initialization when the content given is ``None``.
By default it returns an empty tuple. Overrides if you need to.'''
        return ()

    def __call__(self, environ, start_response, exc_info=None):
        '''Make sure the headers are set.'''
        if not exc_info:
            for rm in self.middleware:
                try:
                    rm(environ, self)
                except:
                    log = pulsar.get_actor().log
                    log.error('Exception in response middleware',
                              exc_info=True)
        start_response(self.status, self.get_headers(), exc_info=exc_info)
        return self
    
    def start(self):
        self.__call__(self.environ, self.start_response)

    def length(self):
        if not self.is_streamed:
            return reduce(lambda x,y: x+len(y), self.content, 0)

    @property
    def response(self):
        return responses.get(self.status_code)

    @property
    def status(self):
        return '%s %s' % (self.status_code, self.response)

    def __str__(self):
        return self.status

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)

    @property
    def is_streamed(self):
        """If the response is streamed (the response is not an iterable with
length information) this property is `True`.  In this case streamed
means that there is no information about the number of iterations.
This is usually `True` if a generator is passed to the response object."""
        return is_streamed(self.content)

    def __iter__(self):
        if self._started:
            raise RuntimeError('WsgiResponse can be iterated only once')
        self._started = True
        if self.is_streamed:
            return wsgi_iterator(self.content, self.encoding)
        else:
            return iter(self.content)

    def __len__(self):
        return len(self.content)

    def set_cookie(self, key, **kwargs):
        """
        Sets a cookie.

        ``expires`` can be a string in the correct format or a
        ``datetime.datetime`` object in UTC. If ``expires`` is a datetime
        object then ``max_age`` will be calculated.
        """
        set_cookie(self.cookies, key, **kwargs)

    def delete_cookie(self, key, path='/', domain=None):
        set_cookie(self.cookies, key, max_age=0, path=path, domain=domain,
                   expires='Thu, 01-Jan-1970 00:00:00 GMT')

    def get_headers(self):
        headers = self.headers
        if has_empty_content(self.status_code, self.method):
            headers.pop('content-type', None)
            headers.pop('content-length', None)
        elif not self.is_streamed:
            cl = 0
            for c in self.content:
                cl += len(c)
            headers['Content-Length'] = str(cl)
        for c in self.cookies.values():
            headers['Set-Cookie'] = c.OutputString()
        return list(headers)


class WsgiHandler(pulsar.LogginMixin):
    '''An handler for application conforming to python WSGI_.

.. attribute:: middleware

    List of callable WSGI middleware callable which accept
    ``environ`` and ``start_response`` as arguments.
    The order matter, since the response returned by the callable
    is the non ``None`` value returned by a middleware.

.. attribute:: response_middleware

    List of functions of the form::

        def ..(environ, start_response, response):
            ...

    where ``response`` is the first not ``None`` value returned by
    the middleware.

'''
    def __init__(self, middleware=None, **kwargs):
        self.setlog(**kwargs)
        if middleware:
            middleware = list(middleware)
        self.middleware = middleware or []
        self.response_middleware = []

    def __call__(self, environ, start_response):
        '''The WSGI callable'''
        response = None
        for middleware in self.middleware:
            response = middleware(environ, start_response)
            if response is not None:
                break
        if response is None:
            raise pulsar.Http404(environ.get('PATH_INFO','/'))
        if hasattr(response, 'middleware'):
            response.middleware.extend(self.response_middleware)
        return response

    def get(self, route='/'):
        '''Fetch a middleware with the given *route*. If it is not found
 return ``None``.'''
        for m in self.middleware:
            if getattr(m,'route',None) == route:
                return m


error_messages = {
    500: 'An exception has occurred while evaluating your request.',
    404: 'Cannot find what you are looking for.'
}

def handle_http_error(connection, response, error=None, encoding='utf-8'):
    '''The default handler for errors while serving an Http requests.

:parameter connection: The :class:`HttpConnection` handling the request.
:parameter response: an instance of :class:`WsgiResponse`.
:parameter e: the exception instance.
:return: bytes to send as response
'''
    response.status_code = getattr(error, 'status', 500)
    msg = error_messages.get(response.status_code, '')
    path = ''
    if response.environ:
        path = ' @ path %s' % response.environ.get('PATH_INFO','/')
    if response.status_code == 500:
        connection.log.critical('Unhandled exception during WSGI response %s',
                                path, exc_info=True)
    else:
        connection.log.info('WSGI %s status code %s',
                            response.status_code, path)
    if has_empty_content(response.status_code):
        msg = ''
        response.content_type = None
    if response.content_type == 'text/html':
        content = textwrap.dedent("""\
        <!DOCTYPE html>
        <html>
          <head>
            <title>{0[reason]}</title>
          </head>
          <body>
            <h1>{0[reason]}</h1>
            {0[msg]}
            <h3>{0[version]}</h3>
          </body>
        </html>
        """).format({"reason": response.status, "msg": msg,
                     "version": pulsar.SERVER_SOFTWARE})
    elif response.content_type == 'application/json':
        content = json.dumps({'message': msg})
    else:
        content = msg
    response.content = content.encode(encoding, 'replace')
    return response