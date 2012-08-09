from io import BytesIO

from pulsar.utils.httpurl import HttpClient, HttpRequest, HttpConnectionPool,\
                                    urlparse, HttpConnection
#from .server import HttpResponse

__all__ = ['HttpTestClient']


class DummyServerConnection(object):

    def __init__(self, data):
        self.data = data

    def write(self, response):
        for data in response:
            if data:
                self.data.append(data)


class DummyClientConnection(HttpConnection):

    @property
    def data_sent(self):
        if not hasattr(self, '_data_sent'):
            self._data_sent = []
        return self._data_sent

    def send(self, data):
        self.data_sent.append(data)

    def getresponse(self):
        return DummyServerConnection(b''.join(self.data_sent))



class HttpTestConnectionPool(HttpConnectionPool):

    def get_connection(self):
        return DummyClientConnection(self.host, self.port)

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


    def _(self):
        for header, value in self.headers:
            name = 'HTTP_%s' % header.upper()
            environ[name] = value

    def __get(self, path, data={}, **extra):
        "Construct a GET request"
        parsed = urlparse(path)
        r = {
            'CONTENT_TYPE': 'text/html; charset=utf-8',
            'PATH_INFO': unquote(parsed[2]),
            'QUERY_STRING': urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'GET',
            'wsgi.input': BytesIO()
        }
        r.update(extra)
        return self.request(**r)

    def __post(self, path, data={}, **extra):
        ''''Construct a POST request'''
        parsed = urlparse(path)
        if isinstance(data, Mapping) and content_type is MULTIPART_CONTENT:
            post_data = encode_multipart(BOUNDARY, data)
        else:
            match = CONTENT_TYPE_RE.match(content_type)
            charset = match.group(1) if match else self.settings.DEFAULT_CHARSET
            post_data = to_bytes(data, encoding=charset)
        r = {
            'CONTENT_LENGTH': len(post_data),
            'CONTENT_TYPE':   content_type,
            'PATH_INFO':      unquote(parsed[2]),
            'QUERY_STRING':   parsed[4],
            'REQUEST_METHOD': 'POST',
            'wsgi.input':     BytesIO(post_data),
        }
        r.update(extra)
        return self.request(**r)

    def __head(self, path, data={}, **extra):
        "Construct a HEAD request."
        parsed = urlparse(path)
        r = {
            'CONTENT_TYPE':    'text/html; charset=utf-8',
            'PATH_INFO':       unquote(parsed[2]),
            'QUERY_STRING':    urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'HEAD',
            'wsgi.input':      BytesIO()
        }
        r.update(extra)
        return self.request(**r)

    def __options(self, path, data={}, **extra):
        "Constrict an OPTIONS request"

        parsed = urlparse(path)
        r = {
            'PATH_INFO':       unquote(parsed[2]),
            'QUERY_STRING':    urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'OPTIONS',
            'wsgi.input':      BytesIO()
        }
        r.update(extra)
        return self.request(**r)

    def __put(self, path, data={}, **extra):
        "Construct a PUT request."
        if content_type is MULTIPART_CONTENT:
            post_data = encode_multipart(BOUNDARY, data)
        else:
            post_data = data

        # Make `data` into a querystring only if it's not already a string. If
        # it is a string, we'll assume that the caller has already encoded it.
        query_string = None
        if not isinstance(data, basestring):
            query_string = urlencode(data, doseq=True)

        parsed = urlparse(path)
        r = {
            'CONTENT_LENGTH': len(post_data),
            'CONTENT_TYPE':   content_type,
            'PATH_INFO':      unquote(parsed[2]),
            'QUERY_STRING':   query_string or parsed[4],
            'REQUEST_METHOD': 'PUT',
            'wsgi.input':     BytesIO(post_data),
        }
        r.update(extra)
        return self.request(**r)

    def __delete(self, path, data={}, **extra):
        "Construct a DELETE request."
        parsed = urlparse(path)
        r = {
            'PATH_INFO':       unquote(parsed[2]),
            'QUERY_STRING':    urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'DELETE',
            'wsgi.input':      BytesIO()
        }
        r.update(extra)
        return self.request(**r)


class HttpTestClient(HttpClient):
    client_version = 'Pulsar-Http-Test-Client'
    request_class = HttpTestClientRequest
    connection_pool = HttpTestConnectionPool

    def __init__(self, test, wsgi_handler, **kwargs):
        self.test = test
        self.wsgi_handler = wsgi_handler
        super(HttpTestClient, self).__init__(**kwargs)
