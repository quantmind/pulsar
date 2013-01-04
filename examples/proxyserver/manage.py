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
                    is_async, is_failure, local_property
from pulsar.apps import wsgi
from pulsar.utils.httpurl import Headers

ENVIRON_HEADERS = ('content-type', 'content-length')
USER_AGENT = 'Pulsar-Proxy-Server'


def x_forwarded_for(environ, headers):
    headers.add_header('x-forwarded-for', environ['REMOTE_ADDR'])
    

class ProxyMiddleware(LocalMixin):
    '''WSGI middleware for an asynchronous proxy server. To perform
processing on headers you can pass a list of ``headers_middleware``.
An headers middleware is a callable which accepts two parameters, the wsgi
*environ* dictionary and the *headers* container.'''
    def __init__(self, user_agent=None, headers_middleware=None):
        self.headers = headers = Headers(kind='client')
        self.headers_middleware = headers_middleware or []
        if user_agent:
            headers['user-agent'] = user_agent
            
    @local_property
    def http_client(self):
        return HttpClient(timeout=0, decompress=False, store_cookies=False,
                          stream=True)
        
    def __call__(self, environ, start_response):
        # The WSGI thing
        uri = environ['RAW_URI']
        if not uri or uri.startswith('/'):
            raise HttpException(status=404)
        wsgi_response = wsgi.WsgiResponse(environ=environ,
                                          start_response=start_response)
        headers = self.request_headers(environ)
        method = environ['REQUEST_METHOD']
        stream = environ.get('wsgi.input') or io.BytesIO()
        target_response = self.http_client.request(method, uri,
                                                   data=stream.getvalue(),
                                                   headers=headers)
        wsgi_response.content = self.response_generator(target_response,
                                                        wsgi_response)
        return wsgi_response
    
    def request_headers(self, environ):
        '''Modify request headers via the list of :attr:`headers_middleware`.
The returned headers will be sent to the target uri.'''
        headers = Headers(kind='client')
        for k in environ:
            if k.startswith('HTTP_'):
                head = k[5:].replace('_','-')
                headers[head] = environ[k]
        for head in ENVIRON_HEADERS:
            k = head.replace('-','_').upper()
            v = environ.get(k)
            if v:
                headers[head] = v
        headers.update(self.headers)
        for middleware in self.headers_middleware:
            middleware(environ, headers)
        return headers
        
    def response_generator(self, response, wsgi_response):
        response = maybe_async(response)
        while is_async(response):
            yield b''
            response = maybe_async(response)
        stream_content = None
        if is_failure(response):
            wsgi_response.status_code = 500
        else:
            wsgi_response.status_code = response.status_code
            wsgi_response.headers.update(response.headers)
            stream_content = response.stream()
        wsgi_response.start()
        if stream_content:
            for content in stream_content:
                yield content


def server(description=None, name='proxy-server', **kwargs):
    description = description or 'Pulsar Proxy Server'
    wsgi_proxy = ProxyMiddleware(user_agent=USER_AGENT,
                                 headers_middleware=[x_forwarded_for])
    app = wsgi.WsgiHandler(middleware=[wsgi_proxy])
    return wsgi.WSGIServer(app, name=name, description=description, **kwargs)


if __name__ == '__main__':
    server().start()
