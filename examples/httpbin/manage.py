'''Pulsar HTTP test application::

    python manage.py

Implementation
======================

.. autoclass:: HttpBin
   :members:
   :member-order: bysource
'''
import os
import sys
import string
from random import choice, random

try:
    from pulsar.utils.pep import ispy3k, range
except ImportError:     # pragma    nocover
    sys.path.append('../../')
    from pulsar.utils.pep import ispy3k, range

from pulsar import (HttpRedirect, HttpException, version, JAPANESE,
                    coroutine_return)
from pulsar.utils.httpurl import Headers, ENCODE_URL_METHODS
from pulsar.utils.html import escape
from pulsar.apps import wsgi, ws
from pulsar.apps.wsgi import route, Html, Json, HtmlDocument, GZipMiddleware
from pulsar.utils.structures import MultiValueDict
from pulsar.utils.system import json

pyversion = '.'.join(map(str, sys.version_info[:3]))
ASSET_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
FAVICON = os.path.join(ASSET_DIR, 'favicon.ico')

if ispy3k:  # pragma nocover
    characters = string.ascii_letters + string.digits
else:   # pragma nocover
    characters = string.letters + string.digits


def template():
    name = os.path.join(ASSET_DIR, 'template.html')
    with open(name, 'r') as file:
        return file.read()


class HttpBin(wsgi.Router):

    def bind_server_event(self, request, event, handler):
        consumer = request.environ['pulsar.connection'].current_consumer()
        consumer.bind_event(event, handler)

    def get(self, request):
        '''The home page of this router'''
        ul = Html('ul')
        for router in sorted(self.routes, key=lambda r: r.creation_count):
            a = router.link(escape(router.route.path))
            li = Html('li', a, ' %s' % router.parameters.get('title', ''))
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
        coroutine_return(GZipMiddleware(10)(request.environ, response))

    @route('cookies', title='Returns cookie data')
    def cookies(self, request):
        response = request.response
        cookies = dict(((c.key, c.value) for c in response.cookies.values()))
        return Json({'cookies': cookies}).http_response(request)

    @route('cookies/set/<name>/<value>', title='Sets a simple cookie',
           defaults={'name': 'package', 'value': 'pulsar'})
    def request_cookies_set(self, request):
        key = request.urlargs['name']
        value = request.urlargs['value']
        request.response.set_cookie(key, value=value)
        request.response.status_code = 302
        request.response.headers['location'] = '/cookies'
        return request.response

    @route('status/<int(min=100,max=505):status>',
           title='Returns given HTTP Status code',
           defaults={'status': 418})
    def status(self, request):
        request.response.content_type = 'text/html'
        raise HttpException(status=request.urlargs['status'])

    @route('response-headers', title='Returns response headers')
    def response_headers(self, request):
        class Gen:
            headers = None

            def __call__(self, server, **kw):
                self.headers = server.headers

            def generate(self):
                # yield a byte so that headers are sent
                yield b''
                # we must have the headers now
                yield json.dumps(dict(self.headers))
        gen = Gen()
        self.bind_server_event(request, 'on_headers', gen)
        request.response.content = gen.generate()
        request.response.content_type = 'application/json'
        return request.response

    @route('basic-auth/<username>/<password>',
           title='Challenges HTTPBasic Auth',
           defaults={'username': 'username', 'password': 'password'})
    def challenge_auth(self, request):
        auth = request.get('http.authorization')
        if auth and auth.authenticated(request.environ, **request.urlargs):
            return Json({'authenticated': True,
                         'username': auth.username}).http_response(request)
        raise wsgi.HttpAuthenticate('basic')

    @route('digest-auth/<username>/<password>/<qop>',
           title='Challenges HTTP Digest Auth',
           defaults={'username': 'username',
                     'password': 'password',
                     'qop': 'auth'})
    def challenge_digest_auth(self, request):
        auth = request.get('http.authorization')
        if auth and auth.authenticated(request.environ, **request.urlargs):
            return Json({'authenticated': True,
                         'username': auth.username}).http_response(request)
        raise wsgi.HttpAuthenticate('digest', qop=[request.urlargs['qop']])

    @route('stream/<int(min=1):m>/<int(min=1):n>',
           title='Stream m chunk of data n times',
           defaults={'m': 300, 'n': 20})
    def request_stream(self, request):
        m = request.urlargs['m']
        n = request.urlargs['n']
        if m*n > 8388608:
            # limit at 8 megabytes of total data
            raise HttpException(status=403)
        stream = ('Chunk %s\n%s\n\n' % (i+1, ''.join((
            choice(characters) for _ in range(m)))) for i in range(n))
        request.response.content = stream
        return request.response

    @route('websocket', title='A web socket graph')
    def request_websocket(self, request):
        data = open(os.path.join(os.path.dirname(__file__),
                                 'assets', 'websocket.html')).read()
        scheme = 'wss' if request.is_secure else 'ws'
        host = request.get('HTTP_HOST')
        data = data % {'address': '%s://%s/graph-data' % (scheme, host)}
        request.response.content_type = 'text/html'
        request.response.content = data
        return request.response

    @route('stats', title='Live server statistics')
    def request_stats(self, request):
        '''Live stats for the server.

        Try sending lots of requests
        '''
        # scheme = 'wss' if request.is_secure else 'ws'
        # host = request.get('HTTP_HOST')
        # address = '%s://%s/stats' % (scheme, host)
        doc = HtmlDocument(title='Live server stats', media_path='/assets/')
        # docs.head.scripts
        return doc.http_response(request)

    @route('expect', method='post', title='Expectation Failed')
    def expectation_failure(self, request):
        stream = request.get('wsgi.input')
        stream.fail()
        return self.info_data_response(request)

    ########################################################################
    #    BENCHMARK ROUTES
    @route('json')
    def bench_json(self, request):
        return Json({'message': "Hello, World!"}).http_response(request)

    @route('plaintext')
    def bench_text(self, request):
        r = request.response
        r.content = 'Hello, World!'
        r.content_type = 'text/plain; charset=utf-8'
        return r

    ########################################################################
    #    INTERNALS
    def info_data_response(self, request, **params):
        data = self.info_data(request, **params)
        return Json(data).http_response(request)

    def info_data(self, request, **params):
        headers = self.getheaders(request)
        data = {'method': request.method,
                'headers': headers,
                'pulsar': self.pulsar_info(request)}
        if request.method in ENCODE_URL_METHODS:
            data['args'] = dict(request.url_data)
        else:
            args, files = yield request.data_and_files()
            jfiles = MultiValueDict()
            for name, parts in files.lists():
                for part in parts:
                    try:
                        part = part.string()
                    except UnicodeError:
                        part = part.base64()
                    jfiles[name] = part
            data.update((('args', dict(args)),
                         ('files', dict(jfiles))))
        data.update(params)
        coroutine_return(data)

    def getheaders(self, request):
        headers = Headers(kind='client')
        for k in request.environ:
            if k.startswith('HTTP_'):
                headers[k[5:].replace('_', '-')] = request.environ[k]
        return dict(headers)

    def pulsar_info(self, request):
        return request.get('pulsar.connection').info()


class Graph(ws.WS):

    def on_message(self, websocket, msg):
        websocket.write(json.dumps([(i, random()) for i in range(100)]))


class Site(wsgi.LazyWsgi):

    def setup(self, environ):
        router = HttpBin('/')
        return wsgi.WsgiHandler([wsgi.clean_path_middleware,
                                 wsgi.authorization_middleware,
                                 wsgi.FileRouter('/favicon.ico', FAVICON),
                                 wsgi.MediaRouter('media', ASSET_DIR,
                                                  show_indexes=True),
                                 ws.WebSocket('/graph-data', Graph()),
                                 router])


def server(description=None, **kwargs):
    description = description or 'Pulsar HttpBin'
    return wsgi.WSGIServer(Site(), description=description, **kwargs)


if __name__ == '__main__':  # pragma    nocover
    server().start()
