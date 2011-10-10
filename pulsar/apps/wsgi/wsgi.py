import os

import pulsar
from pulsar.net.utils import parse_authorization_header


__all__ = ['WsgiHandler']


EMPTY_DICT = {}
EMPTY_TUPLE = ()

    
def authorization(environ, start_response):
    """An `Authorization` middleware."""
    code = 'HTTP_AUTHORIZATION'
    if code in environ:
        header = environ[code]
        return parse_authorization_header(header)

        
class WsgiHandler(pulsar.PickableMixin):
    '''An asynchronous handler for application conforming to python WSGI_.
    
.. attribute: middleware

    List of WSGI middleware. The orther matter.
    
    
.. _WSGI: http://www.python.org/dev/peps/pep-3333/
'''
    def __init__(self, middleware = None, **kwargs):
        self.log = self.getLogger(**kwargs)
        self.middleware = middleware or []
        
    def __call__(self, environ, start_response):
        '''The WSGI callable'''
        #request = self.REQUEST(environ)
        for middleware in self.middleware:
            response = middleware(environ, start_response)
            if response is not None:
                return response
                # if a middleware has return break the loop and return what it
                # returns
        return []
    

