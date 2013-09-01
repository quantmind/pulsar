'''Pulsar HTTP test application::

    python manage.py

Implementation
======================
    
.. autoclass:: HttpBin
   :members:
   :member-order: bysource
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

from pulsar import HttpRedirect, HttpException, version, JAPANESE
from pulsar.utils.pep import ispy3k, to_bytes
from pulsar.utils.httpurl import (Headers, ENCODE_URL_METHODS, WWWAuthenticate,
                                  hexmd5)
from pulsar.utils.html import escape
from pulsar.apps import wsgi
from pulsar.apps.wsgi import route, Html, Json
from pulsar.utils import events
    
pyversion = '.'.join(map(str, sys.version_info[:3]))
ASSET_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')

if ispy3k:  # pragma nocover
    characters = string.ascii_letters + string.digits
else:   # pragma nocover
    characters = string.letters + string.digits
    
    
def template():
    name = os.path.join(ASSET_DIR, 'template.html')
    with open(name,'r') as file:
        return file.read()


class HttpBin(wsgi.Router):
    
    def get(self, request):
        '''The home page of this router'''
        ul = Html('ul')
        for router in sorted(self.routes, key=lambda r: r.creation_count):
            a = router.link(escape(router.route.path))
            li = Html('li', a, ' %s' % router.parameters.get('title',''))
            ul.append(li)
        title = 'Pulsar HttpBin'
        html = request.html_document
        html.head.title = title
        html.head.links.append('/media/httpbin.css')
        html.head.scripts.append('//code.jquery.com/jquery.min.js')
        html.head.scripts.append('/media/httpbin.js')
        ul = ul.render(request)
        body = template() % (title, version, ul, pyversion, JAPANESE)
        html.body.append(body)
        #html.meta.append()
        return html.http_response(request)
        
    @route('get', title='Returns GET data')
    def _get(self, request):
        return self.info_data_response(request)
    
    @route('post', method='post', title='Returns POST data')
    def _post(self, request):
        return self.info_data_response(request)
    
    @route('patch', method='patch', title='Returns PATCH data')
    def _patch(self, request):
        return self.info_data_response(request)
    
    @route('put', method='put', title='Returns PUT data')
    def _put(self, request):
        return self.info_data_response(request)
    
    @route('delete', method='delete', title='Returns DELETE data')
    def _delete(self, request):
        return self.info_data_response(request)
    
    @route('redirect/<int(min=1,max=10):times>', defaults={'times': 5},
           title='302 Redirect n times')
    def redirect(self, request):
        num = request.urlargs['times'] - 1
        if num:
            raise HttpRedirect('/redirect/%s' % num)
        else:
            raise HttpRedirect('/get')
    
    @route('getsize/<int(min=1,max=8388608):size>', defaults={'size': 150000},
           title='Returns a preset size of data (limit at 8MB)')
    def getsize(self, request):
        size = request.urlargs['size']
        data = {'size': size,
                'data': ''.join(('d' for n in range(size)))}
        return self.info_data_response(request, **data)
    
    @route('gzip', title='Returns gzip encoded data')
    def gzip(self, request):
        response = yield self.info_data_response(request, gzipped=True)
        yield wsgi.middleware.GZipMiddleware(10)(request.environ, response)
    
    @route('cookies', title='Returns cookie data')
    def cookies(self, request):
        cookies = {'cookies': request.get('HTTP_COOKIE')}
        return Json(cookies).http_response(request)

    @route('cookies/set/<name>/<value>', title='Sets a simple cookie',
           defaults={'name': 'package', 'value': 'pulsar'})
    def request_cookies_set(self, request):
        key = request.urlargs['name']
        value = request.urlargs['value']
        request.response.set_cookie(key, value=value)
        request.response.status_code = 302
        request.response.headers['location'] = '/cookies'
        return request.response

    @route('status/<int(min=100,max=505):status>', title='Returns given HTTP Status code',
           defaults={'status': 418})
    def status(self, request):
        request.response.content_type = 'text/html'
        raise HttpException(status=request.urlargs['status'])

    @route('response-headers', title='Returns response headers')
    def response_headers(self, request):
        class Gen:
            headers = None
            def __call__(self, headers=None, **kwargs):
                self.headers = headers
            def generate(self):
                #yield a byte so that headers are sent
                yield '{'
                # we must have the headers now
                headers = json.dumps(dict(self.headers))
                yield headers[1:]
        gen = Gen()
        events.bind('http-headers', gen)
        request.response.content = gen.generate()
        request.response.content_type = 'application/json'
        return request.response

    @route('basic-auth/<username>/<password>', title='Challenges HTTPBasic Auth',
           defaults={'username': 'username', 'password': 'password'})
    def challenge_auth(self, request):
        auth = request.get('http.authorization')
        if auth and auth.type == 'basic':
            username = request.urlargs['username']
            password = request.urlargs['password']
            if auth.authenticated(request.environ, username, password):
                return Json({'autheinticated': True,
                             'username': auth.username}).http_response(request)
        h = ('WWW-Authenticate', str(WWWAuthenticate.basic("Fake Realm")))
        raise HttpException(status=401, headers=[h])
        
    @route('digest-auth/<username>/<password>/<qop>',
           title='Challenges HTTP Digest Auth',
           defaults={'username': 'username',
                     'password': 'password',
                     'qop': 'auth'})
    def challenge_digest_auth(self, request):
        auth = request.get('http.authorization')
        if auth and auth.authenticated(environ, *bits[1:]):
            data = jsonbytes({'autheinticated': True,
                              'username': auth.username})
            return self.info_data_response(request, **data)
        nonce = hexmd5(to_bytes('%d' % time.time()) + os.urandom(10))
        digest = WWWAuthenticate.digest("Fake Realm", nonce,
                                        opaque=hexmd5(os.urandom(10)),
                                        qop=request.urlargs['qop'])
        raise HttpException(status=401,
                            headers=[('WWW-Authenticate', str(digest))])
        
    @route('stream/<int(min=1):m>/<int(min=1):n>', title='Stream m chunk of data n times',
           defaults={'m': 300, 'n': 20})
    def request_stream(self, request):
        m = request.urlargs['m']
        n = request.urlargs['n']
        if m*n > 8388608:
            # limit at 8 megabytes of total data
            raise HttpException(status=403)
        stream = ('Chunk %s\n%s\n\n' % (i+1, ''.join((choice(characters) for\
                    _ in range(m)))) for i in range(n))
        request.response.content = stream
        return request.response
    
    ############################################################################
    #    INTERNALS
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
        return data
    
    def getheaders(self, request):
        headers = Headers(kind='client')
        for k in request.environ:
            if k.startswith('HTTP_'):
                headers[k[5:].replace('_','-')] = request.environ[k]
        return dict(headers)
    

class Site(wsgi.LazyWsgi):
    
    def setup(self):
        return wsgi.WsgiHandler([wsgi.clean_path_middleware,
                                 wsgi.cookies_middleware,
                                 wsgi.authorization_middleware,
                                 wsgi.MediaRouter('media', ASSET_DIR),
                                 HttpBin('/')])
        #return validator(app)
    
    
def server(description=None, **kwargs):
    description = description or 'Pulsar HttpBin'
    return wsgi.WSGIServer(Site(), description=description, **kwargs)


if __name__ == '__main__':  #pragma    nocover
    server().start()