'''Classes for testing WSGI servers using the HttpClient'''
from io import BytesIO
import logging
import socket

from pulsar import IStream, create_socket_address
from pulsar.utils.httpurl import HttpClient, HttpRequest, HttpConnectionPool,\
                                    HttpResponse, urlparse, HttpConnection,\
                                    HttpParser
from pulsar.apps.wsgi import server, handle_http_error
#from .server import HttpResponse

__all__ = ['HttpTestClient']


class DummyHttpServerConnection(IStream):
    '''This is a simple class simulating a connection on
a Http server. It contains the client response so that the
write method simply write on the client response
object.'''
    def __init__(self, client_response):
        self.client_response = client_response
        self.parser = HttpParser()
        for d in client_response.request_data:
            self.parser.execute(d, len(d))

    @property
    def wsgi_handler(self):
        return self.client_response.request.client.wsgi_handler
    
    @property
    def server_name(self):
        return 'local-testing-server'
    
    @property
    def server_port(self):
        return 8888

    def handle_http_error(self, response, e):
        return handle_http_error(self, response, e)

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

    def read(self):
        request = self.request
        if not request:
            raise ValueError('request not available')
        # Create the Dummy test connection
        c = DummyHttpServerConnection(self)
        # Get environment
        environ = server.wsgi_environ(c, c.parser)
        # Create the Server response
        self.server_response = server.HttpResponse(c, environ)
        # Write the response
        c.write(self.server_response)
        return self


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

    def __init__(self, client, url, method, ajax=False, **request):
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
