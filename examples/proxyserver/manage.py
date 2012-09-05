'''A HTTP proxy server in Pulsar::

    python manage.py
'''
import io
import json
import sys
try:
    import pulsar
except ImportError:
    sys.path.append('../../')

from pulsar import HttpException, LocalMixin, HttpClient, maybe_async,\
                    is_async, is_failure
from pulsar.apps import wsgi
from pulsar.utils.httpurl import Headers

ENVIRON_HEADERS = ('content-type', 'content-length')
USER_AGENT = 'Pulsar-Proxy-Server'


class ProxyMiddleware(LocalMixin):
    '''WSGI middleware for an asynchronous proxy server. By default it adds the
X-Forwarded-For header. To perform more processing on headers you can add
``headers_middleware``.'''
    def __init__(self, user_agent=None, timeout=0, headers_middleware=None):
        self.headers = headers = Headers(kind='client')
        self.headers_middleware = headers_middleware or []
        self.timeout = timeout
        if user_agent:
            headers['user-agent'] = user_agent
            
    @property
    def http_client(self):
        if 'http' not in self.local:
            self.local.http = HttpClient(timeout=self.timeout,
                                            decompress=False,
                                            store_cookies=False)
        return self.local.http
        
    def __call__(self, environ, start_response):
        uri = environ['RAW_URI']
        if not uri or uri.startswith('/'):
            raise HttpException(status=404)
        wsgi_response = wsgi.WsgiResponse(environ=environ,
                                          start_response=start_response)
        headers = self.request_headers(environ)
        method = environ['REQUEST_METHOD']
        stream = environ.get('wsgi.input') or io.BytesIO()
        d = self.http_client.request(method, uri, data=stream.getvalue(),
                                     headers=headers)
        wsgi_response.content = self.response_generator(d, wsgi_response)
        return wsgi_response
    
    def request_headers(self, environ):
        '''Modify request headers. The returned headers will be sent to the
request uri.'''
        headers = Headers(kind='client')
        for k in environ:
            if k.startswith('HTTP_'):
                head = k[5:].replace('_','-')
                headers[head] = environ[k]
        for head in ENVIRON_HEADERS:
            k = head.replace('-','_').upper()
            if k in environ:
                headers[head] = environ[k]
        headers.update(self.headers)
        headers.add_header('x-forwarded-for', environ['REMOTE_ADDR'])
        for middleware in self.headers_middleware:
            middleware(environ, headers)
        return headers
        
    def response_generator(self, response, wsgi_response):
        response = maybe_async(response)
        while is_async(response):
            yield b''
            response = maybe_async(response)
        content = None
        if is_failure(response):
            wsgi_response.status_code = 500
        else:
            wsgi_response.status_code = response.status_code
            wsgi_response.headers.update(response.headers)
            content = response.content
        wsgi_response.start()
        if content:
            yield content

def server(description=None, name='proxy-server', **kwargs):
    description = description or 'Pulsar Proxy Server'
    app = wsgi.WsgiHandler(middleware=[ProxyMiddleware(user_agent=USER_AGENT)])
    return wsgi.WSGIServer(app, name=name, description=description, **kwargs)


if __name__ == '__main__':
    server().start()
