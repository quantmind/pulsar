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
        
Implemenation
===========================

.. autoclass:: ProxyServerWsgiHandler
   :members:
   :member-order:
   
'''
import io
import sys
try:
    import pulsar
except ImportError:
    sys.path.append('../../')

from pulsar import HttpException
from pulsar.apps import wsgi, http
from pulsar.utils.httpurl import Headers
from pulsar.utils.log import LocalMixin, local_property

ENVIRON_HEADERS = ('content-type', 'content-length')
USER_AGENT = 'Pulsar-Proxy-Server'


def x_forwarded_for(environ, headers):
    '''Add *x-forwarded-for* header'''
    headers.add_header('x-forwarded-for', environ['REMOTE_ADDR'])
    
class user_agent:
    '''Override user-agent header'''
    def __init__(self, agent):
        self.agent = agent
        
    def __call__(self, environ, headers):
        headers['user-agent'] = self.agent

    
class ProxyServerWsgiHandler(LocalMixin):
    '''WSGI middleware for an asynchronous proxy server. To perform
processing on headers you can pass a list of ``headers_middleware``.
An headers middleware is a callable which accepts two parameters, the wsgi
*environ* dictionary and the *headers* container.'''
    def __init__(self, headers_middleware=None):
        self.headers_middleware = headers_middleware or []
    
    @local_property
    def http_client(self):
        return http.HttpClient(decompress=False, store_cookies=False)
        
    def __call__(self, environ, start_response):
        # The WSGI thing
        uri = environ['RAW_URI']
        if not uri or uri.startswith('/'):  # No proper uri, raise 404
            raise HttpException(status=404)
        request_headers = self.request_headers(environ)
        method = environ['REQUEST_METHOD']
        stream = environ.get('wsgi.input') or io.BytesIO()
        response = self.http_client.request(method, uri,
                                            data=stream.getvalue(),
                                            headers=request_headers,
                                            version=environ['SERVER_PROTOCOL'])
        for data in self.generate(environ, start_response, response):
            response.on_finished.raise_all()
            yield data
        
    def generate(self, environ, start_response, response):
        '''Generate response asynchronously'''
        while response.parser is None or not response.parser.is_headers_complete():
            yield b''   # response headers not yet available
        headers = Headers(self.remove_hop_headers(response.headers), kind='server')
        start_response(response.status, list(headers))
        while not response.parser.is_message_complete():
            yield response.recv_body()
        body = response.recv_body()
        if body:
            yield body
    
    def request_headers(self, environ):
        '''Fill request headers from the environ dictionary and
modify the headers via the list of :attr:`headers_middleware`.
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
        for middleware in self.headers_middleware:
            middleware(environ, headers)
        return headers
        
    def remove_hop_headers(self, headers):
        for header, value in headers:
            if header.lower() not in wsgi.HOP_HEADERS:
                yield header, value
                    

def server(name='proxy-server', headers_middleware=None, **kwargs):
    if headers_middleware is None:
        headers_middleware = [user_agent(USER_AGENT), x_forwarded_for]
    wsgi_proxy = ProxyServerWsgiHandler(headers_middleware)
    return wsgi.WSGIServer(wsgi_proxy, name=name, **kwargs)


if __name__ == '__main__':
    server().start()
