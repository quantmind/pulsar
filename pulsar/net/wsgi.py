# -*- coding: utf-8 -
#
# Initial file from gunicorn.
# http://gunicorn.org/
# Adapted for Python 3 compatibility and to work with pulsar
#
# Original GUNICORN LICENCE
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import os
import re
import sys
from functools import partial

import pulsar
from pulsar.utils.tools import cached_property

from .utils import parse_authorization_header


__all__ = ['WsgiHandler','WsgiRequest']


EMPTY_DICT = {}
EMPTY_TUPLE = ()


class WsgiRequest(object):
    
    def __init__(self, environ):
        self.environ = environ
        self.data = environ.get('wsgi.body')
        
    def read(self):
        return self.environ['wsgi.input'].read(self._on_data)
    
    def _on_data(self, data):
        self.data = data
    
    
def authorization(environ, start_response):
    """An `Authorization` middleware."""
    code = 'HTTP_AUTHORIZATION'
    if code in environ:
        header = environ[code]
        return parse_authorization_header(header)

        
class WsgiHandler(pulsar.PickableMixin):
    '''An asynchronous handler for application conforming to python WSGI_.
    
.. attribute: request_middleware

    Optional list of middleware request functions.
    
.. attribute: response_middleware

    Optional list of middleware response functions.
    
    
.. _WSGI: http://www.python.org/dev/peps/pep-3333/
'''    
    def __init__(self, middleware = None, **kwargs):
        self.log = self.getLogger(**kwargs)
        self.middleware = middleware or []
        
    def request(self, environ):
        return WsgiRequest(environ)
    
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
    
    def on_data(self, environ,  start_response, data):
        '''Callback when data is available'''
        pass
    
    def send(self, request, name, args = None, kwargs = None,
             server = None, ack = True):
        worker = request.environ['pulsar.worker']
        if server:
            server = worker.ACTOR_LINKS[server]
        else:
            server = worker.arbiter
        if name in server.remotes:
            ack = server.remotes[name]
        args = args or EMPTY_TUPLE
        kwargs = kwargs or EMPTY_DICT
        return server.send(worker.aid, (args,kwargs), name = name, ack = ack)

