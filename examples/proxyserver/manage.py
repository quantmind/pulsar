'''An asynchronous multi-process `HTTP proxy server`_ with *headers middleware*
to manipulate the original request headers. If the header middleware is
an empty list, the proxy passes requests and responses unmodified.
This is an implementation for a forward-proxy which can be used
to retrieve from any type of source from the Internet.

To run the server::

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
   
   
.. _`HTTP proxy server`: http://en.wikipedia.org/wiki/Proxy_server
'''
import io
import sys
from collections import deque
from functools import partial

try:
    import pulsar
except ImportError:
    sys.path.append('../../')

from pulsar import Deferred, HttpException
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
        wsgi_response = ProxyResponse(start_response)
        response = self.http_client.request(method, uri,
                                            data=stream.getvalue(),
                                            headers=request_headers,
                                            version=environ['SERVER_PROTOCOL'])
        response.on_finished.add_errback(partial(wsgi_response.error, uri))
        response.bind_event('data_processed', wsgi_response)
        return wsgi_response
    
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


class Queue:
    
    def __init__(self):
        self._futures_in = deque()
        self._futures_out = deque()
        
    def size(self):
        return len(self._futures_out)
    
    def put(self, result):
        '''Put a result in the queue.'''
        future = self._futures_in.popleft()
        future.callback(result)
        
    def get(self):
        '''get a future from the queue.'''
        return self._futures_out.popleft()
        
    def add(self):
        '''Add a future to the queue'''
        future = Deferred()
        self._futures_in.append(future)
        self._futures_out.append(future)
        
    
class ProxyResponse(object):
    '''Asynchronous wsgi response.'''
    def __init__(self, start_response):
        self.start_response = start_response
        self.headers = None
        self.futures = Queue()
        self.futures.add()
        
    def __iter__(self):
        while self.futures.size():
            yield self.futures.get()
            
    def __call__(self, response):
        try:
            result = self._body(response)
        except Exception:
            result = sys.exc_info()
        if result is not None:
            if not response.parser.is_message_complete():
                self.futures.add()
            self.futures.put(result)
        
    def error(self, uri, failure):
        '''Handle a failure.'''
        failure.log()
        msg = 'Oops! Could not find %s' % uri
        html = wsgi.HtmlDocument(title=msg)
        html.body.append('<h1>%s</h1>' % msg)
        data = html.render()
        response = wsgi.WsgiResponse(504, data, content_type='text/html',
                                environ={}, start_response=self.start_response)
        response.start()
        self.futures.put(response.content[0])
        
    def _body(self, response):
        if response.parser.is_headers_complete():
            if self.headers is None:
                headers = self.remove_hop_headers(response.headers)
                self.headers = Headers(headers, kind='server')
                # start the response
                self.start_response(response.status, list(self.headers))
            return response.recv_body()
    
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
