import os

import pulsar
from pulsar.utils.http import parse_authorization_header, Headers
from pulsar.net import responses


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
    
.. attribute:: environ

    the dictionary of WSGI enmvironment or a request object
    with ``environ`` as attribute.
    
.. attribute:: start_response

    The ``start_response`` WSGI callable
    
.. attribute:: middleware

    The response middleware iterable
'''
    DEFAULT_STATUS_CODE = 200
    DEFAULT_CONTENT_TYPE = 'text/plain'
    
    def __init__(self, environ, start_response, status = None, content = None,
                 response_headers = None, content_type = None,
                 encoding = None):
        request = None
        if not isinstance(environ,dict):
            if hasattr(environ,'environ'):
                request = environ
                environ = request.environ
            else:
                raise ValueError('Not a valid environment {0}'.format(environ))
        self.status_code = status or self.DEFAULT_STATUS_CODE
        self.request = request
        self.environ = environ
        self._start_response = start_response
        self.content_type = content_type or self.DEFAULT_CONTENT_TYPE
        self.headers = Headers(response_headers)
        if content is None:
            content = self.get_content()
        elif isinstance(content,bytes):
            content = (content,)
        self.content = content
        
    def start_response(self):
        return self._start_response(self.status_code,list(self.headers))
        
    def get_content(self):
        return ()
    
    @property
    def response(self):
        return responses.get(self.status_code)
        
    def __str__(self):
        return '{0} {1}'.format(self.status_code,self.response)
            
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
        
    @property
    def is_streamed(self):
        """If the response is streamed (the response is not an iterable with
a length information) this property is `True`.  In this case streamed
means that there is no information about the number of iterations.
This is usually `True` if a generator is passed to the response object."""
        try:
            len(self)
        except TypeError:
            return True
        return False
        
    def __iter__(self):
        if not self.is_streamed:
            self.start_response()
        return self.content
    
    def __len__(self):
        len(self.content)
        
        
class WsgiHandler(pulsar.LogginMixin):
    '''An handler for application conforming to python WSGI_.
    
.. attribute: middleware

    List of callable WSGI function which accept. The order matter.
    
    
.. _WSGI: http://www.python.org/dev/peps/pep-3333/
'''
    def __init__(self, middleware = None, **kwargs):
        self.setlog(**kwargs)
        if middleware:
            middleware = list(middleware)
        self.middleware = middleware or []
        self.response_middleware = []
        
    def __call__(self, environ, start_response):
        '''The WSGI callable'''
        #request = self.REQUEST(environ)
        for middleware in self.middleware:
            response = middleware(environ, start_response)
            if response is not None:
                if isinstance(response,WsgiResponse):
                    for rm in self.response_middleware:
                        rm(response)
                return response
        return ()
    

