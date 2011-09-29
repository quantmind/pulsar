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

import pulsar
from pulsar.utils.tools import cached_property

from .utils import parse_authorization_header
from .globals import *


__all__ = ['WsgiRequest','WsgiHandler']


EMPTY_DICT = {}
EMPTY_TUPLE = ()


class WsgiRequest(object):
    '''An request environment wrapper'''
    def __init__(self, environ):
        self.environ = environ
        self.sock = environ.get('pulsar.socket')
        self.path = environ.get('PATH_INFO')
        self._init()
    
    def _init(self):
        pass
    
    @property
    def actor(self):
        return self.environ.get("pulsar.worker")
    
    @cached_property
    def data(self):
        return self.environ['wsgi.input'].read()
    
    @cached_property
    def authorization(self):
        """The `Authorization` object in parsed form."""
        code = 'HTTP_AUTHORIZATION'
        if code in self.environ:
            header = self.environ[code]
            return parse_authorization_header(header)

        
class WsgiHandler(pulsar.PickableMixin):
    '''Asyncronous WSGI handler'''
    REQUEST = WsgiRequest
    
    def __init__(self,
                 request_middleware = None,
                 response_middleware = None,
                 **kwargs):
        self.log = self.getLogger(**kwargs)
        self.request_middleware = request_middleware
        self.response_middleware = response_middleware
        self._init(**kwargs)
        
    def _init(self,**kwargs):
        pass
        
    def __call__(self, environ, start_response):
        request = self.REQUEST(environ)
        if self.request_middleware:
            self.request_middleware.apply(request)
        response = self.execute(request, start_response)
        #if self.response_middleware:
        #    self.response_middleware.apply(response)
        return response
        
    def execute(self, request, start_response):
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

