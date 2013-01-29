'''Pulsar HTTP test application::

    python manage.py
'''
import re
import os
import json
import sys
import string
import time
from random import choice
from wsgiref.validate import validator
try:
    import pulsar
except ImportError: #pragma    nocover
    sys.path.append('../../')
    import pulsar
    
    
def home(request):
    for routes in request.cache.router:
        pass
    
def get(request):
    return response(info_data(request))


HttpBin = Router('/',
                 Router('/get', get=get),
                 Router('/post', post=post),
                 Router('/patch', patch=path),
                 Router('/put', put=put),
                 Router('/redirect', get=redirect),
                 Router('/cookies/',
                        Router('/<key>/<value>', get=set_cookie),
                        get=cookies),
                 get=home)


class LazyWsgi(LocalMixin):
    '''We use a Lazy wsgi class because the wsgi validator is not pickable'''
    def __call__(self, environ, start_response):
        handler = self.local.handler
        if handler is None:
            self.local.handler = handler = self.setup()
        return handler(environ, start_response)
    
    def setup(self):
        app = wsgi.WsgiHandler(middleware=(wsgi.clean_path_middleware,
                                           wsgi.cookies_middleware,
                                           wsgi.authorization_middleware,
                                           HttpBin))
        return validator(app)
    
def server(description=None, **kwargs):
    description = description or 'Pulsar HttpBin'
    return wsgi.WSGIServer(LazyWsgi(), description=description, **kwargs)


if __name__ == '__main__':  #pragma    nocover
    server().start()