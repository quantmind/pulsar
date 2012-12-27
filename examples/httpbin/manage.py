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
try:
    import pulsar
except ImportError: #pragma    nocover
    sys.path.append('../../')
    import pulsar
    
from pulsar import HttpRedirect, LocalMixin, local_property, version
from pulsar.apps import wsgi, ws
from pulsar.apps.wsgi.server import HttpResponse
from pulsar.utils.structures import OrderedDict
from pulsar.utils.httpurl import Headers, parse_qs, ENCODE_URL_METHODS,\
                                 responses, has_empty_content, addslash,\
                                 itervalues, range, ispy3k, hexmd5, to_bytes,\
                                 WWWAuthenticate
from pulsar.utils.multipart import parse_form_data
from pulsar.utils import events

pyversion = '.'.join(map(str,sys.version_info[:3]))

error_messages = {404: '<p>The requested URL was not found on the server.</p>'}

if ispy3k:  # pragma nocover
    characters = string.ascii_letters + string.digits
else:   # pragma nocover
    characters = string.letters + string.digits

def jsonbytes(data):
    return json.dumps(data, indent=4).encode('utf-8')

def getheaders(environ):
    headers = Headers(kind='client')
    for k in environ:
        if k.startswith('HTTP_'):
            headers[k[5:].replace('_','-')] = environ[k]
    return dict(headers)

def info_data(environ, **params):
    method = environ['REQUEST_METHOD']
    headers = getheaders(environ)
    args = {}
    if method in ENCODE_URL_METHODS:
        qs = environ.get('QUERY_STRING',{})
        if qs:
            args = parse_qs(qs)
    else:
        post, files = parse_form_data(environ)
        args = dict(post)
    data = {'method': method, 'headers': headers, 'args': args}
    data.update(params)
    return jsonbytes(data)


class HttpException(pulsar.HttpException):
    pass


class route(object):
    counter = 0
    def __init__(self, url, method='GET', title='?', params=None):
        cls = self.__class__
        cls.counter += 1
        self.order = cls.counter
        self.method = method.upper()
        self.title = title
        url = addslash(url)
        href = url
        self.params = []
        self.key = tuple((b for b in url.split('/') if b))
        if params:
            if url.endswith('/'):
                url = url[:-1]
            href = url
            for name, value in params:
                url += '/:%s' % name
                href += '/%s' % value
                self.params.append(name)
        self.url = url
        self.href = href

    def render(self):
        return "<li><a href='%s' class='%s'>%s</a> %s.</li>" %\
                 (self.href, self.method.lower(), self.url, self.title)

    def __call__(self, f):
        def _(obj, environ, *args, **kwargs):
            if self.method != environ['REQUEST_METHOD']:
                raise HttpException(status=405)
            else:
                return f(obj, environ, *args, **kwargs)
        _.route = self
        return _


class HttpBin(LocalMixin):
    '''WSGI application running on the server'''
    @local_property
    def routes(self):
        return OrderedDict(((r.route.key, r) for r in sorted(
                                self._routes(), key=lambda x: x.route.order)))

    @classmethod
    def _routes(cls):
        for m in cls.__dict__:
            attr = getattr(cls, m)
            if hasattr(attr, 'route'):
                yield attr

    def __call__(self, environ, start_response):
        response = self.request(environ)
        return response(environ, start_response)

    def request(self, environ):
        try:
            path = environ.get('PATH_INFO')
            if not path or path == '/':
                return self.home(environ)
            else:
                path = path[1:]
                leaf = True
                if path.endswith('/'):
                    leaf = False
                    path = path[:-1]
                keys = tuple(path.split('/'))
                bits = []
                route = None
                while keys:
                    route = self.routes.get(keys)
                    if route:
                        break
                    bits.insert(0, keys[-1])
                    keys = keys[:-1]
                if route is None:
                    raise HttpException(status=404)
                return route(self, environ, bits)
        except HttpException as e:
            status = e.status
            if has_empty_content(status, environ['REQUEST_METHOD']):
                return wsgi.WsgiResponse(status, response_headers=e.headers)
            else:
                content = error_messages.get(status,'')
                title = responses.get(status)
                content = '<h1>%s - %s</h1>%s' % (status, title, content)
                return self.render(content, title=title, status=status,
                                   headers=e.headers)

    def load(self, name):
        name = os.path.join(os.path.dirname(os.path.abspath(__file__)),name)
        with open(name,'r') as file:
            return file.read()

    def redirect(self, location='/'):
        raise HttpRedirect(location)

    def response(self, data, status=200, content_type=None, headers=None):
        content_type = content_type or 'application/json'
        return wsgi.WsgiResponse(status,
                                 content=data,
                                 content_type=content_type,
                                 response_headers=headers)

    def render(self, body, title=None, status=200, headers=None):
        template = self.load('template.html')
        title = title or 'Pulsar HttpBin'
        html = (template % (title, version, body, pyversion)).encode('utf-8')
        return self.response(html, status=status, content_type='text/html',
                             headers=headers)

    # ROUTE FUNCTIONS

    def home(self, environ):
        data = '<ul>\n%s\n</ul>' % '\n'.join(
                            (r.route.render() for r in itervalues(self.routes)))
        return self.render(data)

    @route('get', title='Returns GET data')
    def request_get(self, environ, bits):
        if bits:
            raise HttpException(status=404)
        return self.response(info_data(environ))
    
    @route('getsize', title='Returns a preset size of data',
           params=[('size', int(2.5*HttpResponse.MAX_CHUNK))])
    def request_getsize(self, environ, bits):
        if len(bits) == 1:
             size = int(bits[0])
        else:
            raise HttpException(status=404)
        data = {'size': size,
                'data': ''.join(('d' for n in range(size)))}
        return self.response(jsonbytes(data))

    @route('post', method='POST', title='Returns POST data')
    def request_post(self, environ, bits):
        if bits:
            raise HttpException(status=404)
        return self.response(info_data(environ))

    @route('put', method='PUT', title='Returns PUT data')
    def request_put(self, environ, bits):
        if bits:
            raise HttpException(status=404)
        return self.response(info_data(environ))
    
    @route('patch', method='PATCH', title='Returns PATCH data')
    def request_patch(self, environ, bits):
        if bits:
            raise HttpException(status=404)
        return self.response(info_data(environ))
    
    @route('delete', method='DELETE', title='Returns DELETE data')
    def request_delete(self, environ, bits):
        if bits:
            raise HttpException(status=404)
        return self.response(info_data(environ))

    @route('redirect', title='302 Redirect n times', params=[('n', 6)])
    def request_redirect(self, environ, bits):
        if bits:
            if len(bits) > 2:
                raise HttpException(status=404)
            num = int(bits[0])
        else:
            num = 1
        num -= 1
        if num > 0:
            return self.redirect('/redirect/%s' % num)
        else:
            return self.redirect('/get')

    @route('gzip', title='Returns gzip encoded data')
    def request_gzip(self, environ, bits):
        if bits:
            raise HttpException(status=404)
        data = self.response(info_data(environ, gzipped=True))
        middleware = wsgi.middleware.GZipMiddleware(10)
        # Apply the gzip middleware
        middleware(environ, data)
        return data

    @route('cookies', title='Returns cookie data')
    def request_cookies(self, environ, bits):
        cookies = {'cookies': environ['HTTP_COOKIE']}
        return self.response(jsonbytes(cookies))

    @route('cookies/set', 'GET', 'Sets a simple cookie',
           params=(('name', 'package'), ('value', 'pulsar')))
    def request_cookies_set(self, environ, bits):
        if len(bits) == 2:
            key = bits[0]
            value = bits[1]
            if key and value:
                response = wsgi.WsgiResponse(302,
                                    response_headers=[('location','/cookies')])
                response.set_cookie(key, value=value)
                return response
        else:
            raise HttpException(status=404)
        cookies = {'cookies': environ['HTTP_COOKIE']}
        return self.response(jsonbytes(cookies))

    @route('status','GET', 'Returns given HTTP Status code',
           params=[('status', 418)])
    def request_status(self, environ, bits):
        try:
            number = int(bits[0]) if len(bits) == 1 else 404
        except:
            raise HttpException(status=404)
        raise HttpException(status=number)

    @route('response-headers', title='Returns response headers')
    def request_response_headers(self, environ, bits):
        if bits:
            raise HttpException(status=404)

        class Gen:
            headers = None
            def __call__(self, headers=None, **kwargs):
                self.headers = headers
            def generate(self):
                #yield a byte so that headers are sent
                yield '{'
                # we must have the headers now
                headers = jsonbytes(dict(self.headers))
                yield headers[1:]
        gen = Gen()
        events.bind('http-headers', gen)
        data = wsgi.WsgiResponse(
                            200,
                            content=gen.generate(),
                            content_type='application/json')
        return data

    @route('basic-auth', title='Challenges HTTPBasic Auth',
           params=(('username','username'),('password','password')))
    def request_challenge_auth(self, environ, bits):
        if len(bits) == 2:
            auth = environ.get('HTTP_AUTHORIZATION')
            if auth and auth.type == 'basic':
                if auth.authenticated(environ, *bits):
                    data = jsonbytes({'autheinticated': True,
                                      'username': auth.username})
                    return self.response(data)
            h = ('WWW-Authenticate', str(WWWAuthenticate.basic("Fake Realm")))
            raise HttpException(status=401, headers=[h])
        else:
            raise HttpException(status=404)
        
    @route('digest-auth', title='Challenges HTTP Digest Auth',
           params=(('qop', 'auth'),('username','username'),
                   ('password','password')))
    def request_challenge_digest_auth(self, environ, bits):
        if len(bits) == 3:
            auth = environ.get('HTTP_AUTHORIZATION')
            if auth and auth.authenticated(environ, *bits[1:]):
                data = jsonbytes({'autheinticated': True,
                                  'username': auth.username})
                return self.response(data)
            nonce = hexmd5(to_bytes('%d' % time.time()) + os.urandom(10))
            digest = WWWAuthenticate.digest("Fake Realm", nonce,
                                            opaque=hexmd5(os.urandom(10)),
                                            qop=bits[:1])
            raise HttpException(status=401,
                                headers=[('WWW-Authenticate', str(digest))])
        else:
            raise HttpException(status=404)
        
    @route('stream', title='Stream m chunk of data n times',
           params=(('m', 300), ('n', 20)))
    def request_stream(self, environ, bits):
        if len(bits) == 2:
            try:
                m = int(bits[0])
                n = int(bits[1])
            except:
                raise HttpException(status=404)
        else:
            raise HttpException(status=404)
        stream = ('Chunk %s\n%s\n\n' % (i+1, ''.join((choice(characters) for\
                    _ in range(m)))) for i in range(n))
        return self.response(stream, content_type='text/plain')


class handle(ws.WS):

    def on_message(self, msg):
        path = self.path
        if path == '/echo':
            return msg
        elif path == '/streaming':
            return json.dumps([(i,random()) for i in range(100)])


def server(description=None, **kwargs):
    description = description or 'Pulsar HttpBin'
    app = wsgi.WsgiHandler(middleware=(wsgi.clean_path_middleware,
                                       wsgi.cookies_middleware,
                                       wsgi.authorization_middleware,
                                       HttpBin(),))
    return wsgi.WSGIServer(app, description=description, **kwargs)


if __name__ == '__main__':  #pragma    nocover
    server().start()
