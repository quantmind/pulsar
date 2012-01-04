import os
import logging
from functools import partial

import pulsar
from pulsar import make_async, Deferred, raise_failure, NOT_DONE
from pulsar.utils.http import parse_authorization_header, Headers,\
                                SimpleCookie, set_cookie
from pulsar.net import responses

from .middleware import is_streamed


__all__ = ['WsgiHandler','WsgiResponse']


default_logger = logging.getLogger('pulsar.apps.wsgi')

EMPTY_DICT = {}
EMPTY_TUPLE = ()

    
def authorization(environ, start_response):
    """An `Authorization` middleware."""
    code = 'HTTP_AUTHORIZATION'
    if code in environ:
        header = environ[code]
        return parse_authorization_header(header)


def generate_content(gen):
    for data in gen:
        if data is NOT_DONE:
            yield b''
        elif isinstance(data,bytes):
            yield data
        elif isinstance(data,str):
            yield data.encode('utf-8')
        else:
            for b in generate_content(data):
                yield b
                

class WsgiResponse(object):
    '''A WSGI response wrapper initialized by a WSGI request middleware.
Instances are callable using the standard WSGI call::

    response = WsgiResponse(200)
    response(environ, start_response)
    

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
        
        if content is None:
            content = self.default_content()
        elif isinstance(content, bytes):
            content = (content,)
        
        self._content = None
        self._content_generator = None
        if is_streamed(content):
            self._content_generator = content
        else:
            self.content = content
        
    @property
    def logger(self):
        return self.environ['pulsar.actor'].log if self.environ\
                         else default_logger
    
    def default_content(self):
        '''Called during initialization when the content given is ``None``.
By default it returns an empty tuple. Overrides if you need to.'''
        return ()
    
    def __call__(self, environ, start_response):
        if self.content is None:
            raise ValueError('No content available')
        headers = self.headers
        for c in self.cookies.values():
            headers['Set-Cookie'] = c.OutputString()
        headers = list(headers)
        start_response(self.status, headers)
        return self
        
    @property
    def response(self):
        return responses.get(self.status_code)
    
    @property
    def status(self):
        return '{0} {1}'.format(self.status_code,self.response)
    
    def __get_content(self):
        return self._content
    def __set_content(self, content):
        self.on_content(content)
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
        
    def _generator(self):
        #Called by the __iter__ method when the response is streamed.
        content = []
        try:
            for b in generate_content(self._content_generator):
                if b:
                    content.append(b)
                else:
                    yield b'' # release the eventloop
        except Exception as e:
            pulsar.get_actor().cfg.handle_http_error(self, e)
        else:
            self.content = content
        for c in self.content:
            yield c
                
    def __iter__(self):
        # we have a content generator
        if self._content_generator:
            return self._generator()
        else:
            return iter(self.content)
    
    def __len__(self):
        return len(self.content)
        
    def on_content(self, content):
        cl = 0
        if content is None:
            content = ()
        else:
            if isinstance(content,bytes):
                content = (content,)
            for c in content:
                cl += len(c)
        self.headers['Content-Length'] = str(cl)
        if self.content_type:
            self.headers['Content-type'] = self.content_type
        self._content_generator = None
        self._content = content
        if not self.when_ready.called:
            self.when_ready.callback(self)
    
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
        if hasattr(response,'status_code'):
            for rm in self.response_middleware:
                rm(environ, start_response, response)
            if hasattr(response,'__call__'):
                return response(environ, start_response)
        else:
            pass
        return response
    
    def get(self, route = '/'):
        '''Fetch a middleware with the given *route*. If it is not found
 return ``None``.'''
        for m in self.middleware:
            if getattr(m,'route',None) == route:
                return m

