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

from pulsar import HttpRedirect, version, JAPANESE
from pulsar.utils.httpurl import Headers, ENCODE_URL_METHODS
from pulsar.apps import wsgi
from pulsar.apps.wsgi import route, Html
    
pyversion = '.'.join(map(str, sys.version_info[:3]))
ASSET_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')

def template():
    name = os.path.join(ASSET_DIR, 'template.html')
    with open(name,'r') as file:
        return file.read()
        
class Json(wsgi.AsyncString):
    content_type = 'application/json'


class HttpBin(wsgi.Router):
    
    def get(self, request):
        '''The home page of this router'''
        ul = Html('ul')
        for route in self.routes:
            a = route.link()
            li = Html('li', a, route.parameters.get('title',''))
            ul.append(li)
        title = 'Pulsar HttpBin'
        html = request.html_document
        html.head.title = title
        html.head.links.append('/media/httpbin.css')
        html.head.scripts.append('http://code.jquery.com/jquery.min.js')
        html.head.scripts.append('/media/httpbin.js')
        ul = ul.render(request)
        body = template() % (title, version, ul, pyversion, JAPANESE)
        html.body.append(body)
        #html.meta.append()
        return html.http_response(request)
        
    @route('get')
    def _get(self, request):
        return self.info_data_response(request)
    
    @route('post', method='post')
    def _post(self, request):
        return self.info_data_response(request)
    
    @route('patch', method='patch')
    def _patch(self, request):
        return self.info_data_response(request)
    
    @route('put', method='put')
    def _put(self, request):
        return self.info_data_response(request)
    
    @route('redirect/<int(min=1,max=10):times>', defaults={'times': 5},
           title='302 Redirect n times')
    def redirect(self, request):
        num = request.urlargs['times'] - 1
        if num:
            raise HttpRedirect('/redirect/%s' % num)
        else:
            raise HttpRedirect('/get')
    
    @route('getsize/<int(min=1,max=2000000):size>', defaults={'size': 150000},
           title='Returns a preset size of data')
    def getsize(self, request):
        size = request.urlargs['size']
        data = {'size': size,
                'data': ''.join(('d' for n in range(size)))}
        return self.info_data_response(request, **data)
    
    @route('gzip', title='Returns gzip encoded data')
    def request_gzip(self, request):
        data = self.info_data(request, gzipped=True)
        middleware = wsgi.middleware.GZipMiddleware(10)
        # Apply the gzip middleware
        middleware(request.environ, data)
        return Json(data).http_response(request)
    
    def info_data_response(self, request, **params):
        data = self.info_data(request, **params)
        return Json(data).http_response(request)
    
    def info_data(self, request, **params):
        headers = self.getheaders(request)
        args = {}
        if request.method in ENCODE_URL_METHODS:
            args = request.url_data
        else:
            args = request.body_data
        data = {'method': request.method,
                'headers': headers,
                'args': dict(args)}
        data.update(params)
        return json.dumps(data)
    
    def getheaders(self, request):
        headers = Headers(kind='client')
        for k in request.environ:
            if k.startswith('HTTP_'):
                headers[k[5:].replace('_','-')] = request.environ[k]
        return dict(headers)
    

class Site(wsgi.LazyWsgi):
    
    def setup(self):
        app = wsgi.WsgiHandler(middleware=(wsgi.clean_path_middleware,
                                           wsgi.cookies_middleware,
                                           wsgi.authorization_middleware,
                                           wsgi.MediaRouter('media', ASSET_DIR),
                                           HttpBin('/')))
        return validator(app)
    
    
def server(description=None, **kwargs):
    description = description or 'Pulsar HttpBin'
    return wsgi.WSGIServer(Site(), description=description, **kwargs)


if __name__ == '__main__':  #pragma    nocover
    server().start()