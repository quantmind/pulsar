import os
from functools import partial

import pulsar
from pulsar import make_async, Deferred, raise_failure
from pulsar.utils.http import parse_authorization_header, Headers,\
                                SimpleCookie, set_cookie
from pulsar.net import responses

from .middleware import is_streamed


__all__ = ['WsgiHandler','WsgiResponse']


EMPTY_DICT = {}
EMPTY_TUPLE = ()

    
def authorization(environ, start_response):
    """An `Authorization` middleware."""
    code = 'HTTP_AUTHORIZATION'
    if code in environ:
        header = environ[code]
        return parse_authorization_header(header)


class WsgiResponse(object):
    '''A WSGI response wrapper initialized by a WSGI request middleware.
Instances are callable using the standard WSGI call::

    response = WsgiResponse(200)
    response(environ,start_response)
    

.. attribute:: status_code

    Integer indicating the HTTP status, (i.e. 200)
    
.. attribute:: response

    String indicating the HTTP status (i.e. 'OK')
    
.. attribute:: status

    String indicating the HTTP status code and response (i.e. '200 OK')
    
.. attribute:: environ

    The dictionary of WSGI environment if passed to the constructor.

'''
    DEFAULT_STATUS_CODE = 200
    DEFAULT_CONTENT_TYPE = 'text/plain'
    
    def __init__(self, status = None, content = None,
                 response_headers = None, content_type = None,
                 encoding = None, environ = None):
        request = None
        if environ and not isinstance(environ,dict):
            if hasattr(environ,'environ'):
                request = environ
                environ = request.environ
            else:
                raise ValueError('Not a valid environment {0}'.format(environ))
        self.status_code = status or self.DEFAULT_STATUS_CODE
        self.request = request
        self.environ = environ
        self.cookies = SimpleCookie()
        self.content_type = content_type or self.DEFAULT_CONTENT_TYPE
        self.headers = Headers(response_headers)
        self.when_ready = Deferred()
        orig_content = content
        if content is None:
            content = self.get_content()
        elif isinstance(content,bytes):
            content = (content,)
        self._content_generator = content
        self._content = None
        if not self.is_streamed:
            self.on_content(orig_content)
        
    def __call__(self, environ, start_response):
        headers = self.headers
        for c in self.cookies.values():
            headers['Set-Cookie'] = c.OutputString()
        headers = list(headers)
        start_response(self.status, headers)
        return self
        
    def get_content(self):
        return ()
    
    @property
    def response(self):
        return responses.get(self.status_code)
    
    @property
    def status(self):
        return '{0} {1}'.format(self.status_code,self.response)
    
    def __get_content(self):
        c = self._content
        if c is None:
            return self._content_generator
        elif isinstance(c,bytes):
            return (c,)
        else:
            return c
    def __set_content(self, c):
        if self._content is None:
            raise AttributeError('Cannot set content')
        else:
            if isinstance(c,bytes):
                self.headers['Content-Length'] = str(len(c))
            self._content = c
    content =  property(__get_content, __set_content)
    
    def __str__(self):
        return self.status
            
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
        
    @property
    def is_streamed(self):
        """If the response is streamed (the response is not an iterable with
a length information) this property is `True`.  In this case streamed
means that there is no information about the number of iterations.
This is usually `True` if a generator is passed to the response object."""
        return is_streamed(self.content)
        
    def __iter__(self):
        return iter(self.content)
    
    def __len__(self):
        return len(self.content)
        
    def on_content(self, content):
        self.headers['Content-type'] = self.content_type
        if isinstance(content,bytes):
            self.headers['Content-Length'] = str(len(content))
        self._content = content
        self.when_ready.callback(self)
        return self._content
    
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
        
        
class WsgiHandler(pulsar.LogginMixin):
    '''An handler for application conforming to python WSGI_.
    
.. attribute: middleware

    List of callable WSGI middleware functions which accept
    ``environ`` and ``start_response`` as arguments.
    The order matter.
    
.. attribute:: response_middleware

    List of functions of the form::
    
        def ..(environ, start_response, response):
            ...
            
    where ``response`` is the first not ``None`` value returned by
    the wsgi middleware.

'''
    msg404 = 'Could not find what you are looking for. Sorry.'
    def __init__(self, middleware = None, msg404 = None, **kwargs):
        self.setlog(**kwargs)
        if middleware:
            middleware = list(middleware)
        self.msg404 = msg404 or self.msg404
        self.middleware = middleware or []
        self.response_middleware = []
        
    def __call__(self, environ, start_response):
        '''The WSGI callable'''
        for middleware in self.middleware:
            response = middleware(environ, start_response)
            if response is not None:
                if hasattr(response,'when_ready'):
                    process = partial(self.process_response,
                                      environ,
                                      start_response)
                    response.when_ready.add_callback(process)\
                                       .add_callback(raise_failure)
                else:
                    self.process_response(environ, start_response, response)
                return response
        response =  WsgiResponse(404, content = self.msg404.encode('utf-8'))
        self.process_response(environ, start_response, response)
        return response
                                    
    def process_response(self, environ, start_response, response):
        for rm in self.response_middleware:
            rm(environ, start_response, response)
        if hasattr(response,'__call__'):
            return response(environ, start_response)
        return self
    

