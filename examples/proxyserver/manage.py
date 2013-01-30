'''An asynchronous multi-process HTTP proxy server with *headers middleware*
to manipulate the original request headers. To run the server::

    python manage.py
    
An header middleware is a callable which receives the wsgi *environ* and
the list of request *headers*. By default the example uses:

.. autofunction:: x_forwarded_for

To run with different headers middleware create a new script and do::

    from proxyserver.manage import server
    
    if __name__ == '__main__':
        server(headers_middleware=[...]).start()
        
'''
import io
import sys
try:
    import pulsar
except ImportError:
    sys.path.append('../../')

from pulsar.apps import wsgi
from pulsar.utils.httpurl import Headers
from pulsar.utils.log import LocalMixin, local_property

ENVIRON_HEADERS = ('content-type', 'content-length')
USER_AGENT = 'Pulsar-Proxy-Server'


def x_forwarded_for(environ, headers):
    '''Add *x-forwarded-for* header'''
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
        return wsgi.HttpClient(decompress=False, store_cookies=False)
        
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
        response = self.http_client.request(method, uri,
                                            data=stream.getvalue(),
                                            headers=headers)
        return self.response_generator(wsgi_response, response)
    
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
        
    def response_generator(self, wsgi_response, response):
        parser = response.parser
        while not parser.is_headers_complete():
            yield b''
        wsgi_response.status_code = response.status_code
        wsgi_response.headers.update(response.headers)
        wsgi_response.start()
        while not parser.is_message_complete():
            body = parser.recv_body()
            yield body
        body = parser.recv_body()
        if body:
            yield body
            

def server(description=None, name='proxy-server',
           headers_middleware=None, **kwargs):
    description = description or 'Pulsar Proxy Server'
    headers_middleware = headers_middleware or [x_forwarded_for]
    wsgi_proxy = ProxyMiddleware(user_agent=USER_AGENT,
                                 headers_middleware=headers_middleware)
    app = wsgi.WsgiHandler(middleware=[wsgi_proxy])
    return wsgi.WSGIServer(app, name=name, description=description, **kwargs)


if __name__ == '__main__':
    server().start()
