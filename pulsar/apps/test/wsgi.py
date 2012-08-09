'''Classes for testing WSGI servers using the HttpClient'''
from io import BytesIO

from pulsar.utils.httpurl import HttpClient, HttpRequest, HttpConnectionPool,\
                                    HttpResponse, urlparse, HttpConnection,\
                                    HttpParser
from pulsar.apps.wsgi import server
#from .server import HttpResponse

__all__ = ['HttpTestClient']


class TestHttpServerConnection(object):
    '''This is a simple class simulating a client connection on
a Http server.'''
    address = ('0.0.0.0',0)
    
    def __init__(self, client_response):
        self.client_response = client_response
        self.parser = HttpParser()
        for d in client_response.request_data:
            self.parser.execute(d, len(d))
        
    @property
    def wsgi_handler(self):
        return self.client_response.request.client.wsgi_handler
    
    def write(self, response):
        for data in response:
            if data:
                self.client_response.parsedata(data)
    

class TestHttpResponse(HttpResponse):
    request = None
    server_response = None
    
    def __init__(self, data):
        self.request_data = data
    
    @property
    def environ(self):
        if self.server_response:
            return self.server_response.environ
        
    def check(self):
        request = self.request
        test = request.client.test
        test.assertEqual(request.status_code, self.status_code)
        
    def read(self):
        request = self.request
        if not request:
            raise ValueError('request not available')
        c = TestHttpServerConnection(self)
        environ = server.wsgi_environ(c)
        self.server_response = server.HttpResponse(c, environ)
        c.write(self.server_response)
        self.check()
        

class TestHttpConnection(HttpConnection):

    @property
    def data_sent(self):
        if not hasattr(self, '_data_sent'):
            self._data_sent = []
        return self._data_sent

    def send(self, data):
        self.data_sent.append(data)

    def getresponse(self):
        return TestHttpResponse(self.data_sent)


class HttpTestConnectionPool(HttpConnectionPool):

    def get_connection(self):
        return TestHttpConnection(self.host, self.port)

    def release(self, connection):
        pass

    def remove(self, connection):
        pass


class HttpTestClientRequest(HttpRequest):

    def __init__(self, client, url, method, status_code=200, ajax=False,
                 **request):
        self.status_code = status_code
        super(HttpTestClientRequest, self).__init__(client, url, method,
                                                    **request)
        if ajax:
            self.add_header('x_requested_with', 'XMLHttpRequest')


class HttpTestClient(HttpClient):
    client_version = 'Pulsar-Http-Test-Client'
    request_class = HttpTestClientRequest
    connection_pool = HttpTestConnectionPool

    def __init__(self, test, wsgi_handler, **kwargs):
        self.test = test
        self.wsgi_handler = wsgi_handler
        super(HttpTestClient, self).__init__(**kwargs)
