from pulsar.utils.py2py3 import BytesIO, ispy3k, native_str

if ispy3k:
    from urllib.parse import urlparse, urlunparse, urlsplit, unquote,\
                             urlencode
    from http.cookies import SimpleCookie
else:
    from Cookie import SimpleCookie
    from urlparse import urlparse, urlunparse, urlsplit, unquote
    from urllib import urlencode
    

BOUNDARY = 'BoUnDaRyStRiNg'
MULTIPART_CONTENT = 'multipart/form-data; boundary=%s' % BOUNDARY


def encode_multipart(boundary, data):
    """
    Encodes multipart POST data from a dictionary of form values.

    The key will be used as the form data name; the value will be transmitted
    as content. If the value is a file, the contents of the file will be sent
    as an application/octet-stream; otherwise, str(value) will be sent.
    """
    lines = []
    # Not by any means perfect, but good enough for our purposes.
    is_file = lambda thing: hasattr(thing, "read") and\
                             hasattr(thing.read,'__call__')

    # Each bit of the multipart form data could be either a form value or a
    # file, or a *list* of form values and/or files. Remember that HTTP field
    # names can be duplicated!
    for (key, value) in data.items():
        value = native_str(value)
        if is_file(value):
            lines.extend(encode_file(boundary, key, value))
        elif not isinstance(value,str) and hasattr(value,'__iter__'):
            for item in value:
                if is_file(item):
                    lines.extend(encode_file(boundary, key, item))
                else:
                    lines.extend([
                        '--' + boundary,
                        'Content-Disposition: form-data; name="%s"' % to_str(key),
                        '',
                        item
                    ])
        else:
            lines.extend([
                '--' + boundary,
                'Content-Disposition: form-data; name="{0}"'.format(key),
                '',
                str(value)
            ])

    lines.extend([
        '--' + boundary + '--',
        '',
    ])
    return '\r\n'.join(lines)


def encode_file(boundary, key, file):
    to_str = lambda s: smart_str(s, settings.DEFAULT_CHARSET)
    content_type = mimetypes.guess_type(file.name)[0]
    if content_type is None:
        content_type = 'application/octet-stream'
    return [
        '--' + boundary,
        'Content-Disposition: form-data; name="%s"; filename="%s"' \
            % (to_str(key), to_str(os.path.basename(file.name))),
        'Content-Type: %s' % content_type,
        '',
        file.read()
    ]
    

def fake_input(data = None):
    data = data or ''
    if not isinstance(data,bytes):
        data = data.encode('utf-8')
    return BytesIO(data)


class Response(object):
    
    def __init__(self, environ):
        self.environ = environ
        self.status = None
        self.response_headers = None
        self.exc_info = None
        self.response = None
        
    def __call__(self, status, response_headers, exc_info=None):
        '''Mock the wsgi start_response callable'''
        self.status = status
        self.response_headers = response_headers
        self.exc_info = exc_info
        
    @property
    def status_code(self):
        if self.status:
            return int(self.status.split()[0])


class HttpTestClientRequest(object):
    """
    Class that lets you create mock HTTP environment objects for use in testing.

    Usage:

    rf = HttpTestRequest()
    get_request = rf.get('/hello/')
    post_request = rf.post('/submit/', {'foo': 'bar'})

    Once you have a request object you can pass it to any view function,
    just as if that view had been hooked up using a URLconf.
    """
    def __init__(self, handler, **defaults):
        self.defaults = defaults
        self.cookies = SimpleCookie()
        self.errors = BytesIO()
        self.handler = handler
        self.response_data = None

    def _base_environ(self, ajax = False, **request):
        """
        The base environment for a request.
        """
        environ = {
            'HTTP_COOKIE': self.cookies.output(header='', sep='; '),
            'PATH_INFO': '/',
            'QUERY_STRING': '',
            'REMOTE_ADDR': '127.0.0.1',
            'REQUEST_METHOD': 'GET',
            'SCRIPT_NAME': '',
            'SERVER_NAME': 'testserver',
            'SERVER_PORT': '80',
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'wsgi.version': (1,1),
            'wsgi.url_scheme': 'http',
            'wsgi.errors': self.errors,
            'wsgi.multiprocess': False,
            'wsgi.multithread': False,
            'wsgi.run_once': False,
        }
        environ.update(self.defaults)
        environ.update(request)
        if ajax:
            environ['HTTP_X_REQUESTED_WITH'] = 'XMLHttpRequest'
        return environ

    def request(self, **request):
        "Construct a generic wsgi environ object."
        environ = self._base_environ(**request)
        r = Response(environ)
        r.response = self.handler(environ,r)
        return r
        
    def get(self, path, data={}, ajax = False, **extra):
        "Construct a GET request"
        parsed = urlparse(path)
        r = {
            'CONTENT_TYPE':    'text/html; charset=utf-8',
            'PATH_INFO':       unquote(parsed[2]),
            'QUERY_STRING':    urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'GET',
            'wsgi.input':      fake_input()
        }
        r.update(extra)
        return self.request(**r)
    
    def post(self, path, data={}, content_type=MULTIPART_CONTENT, **extra):
        "Construct a POST request."
        if content_type is MULTIPART_CONTENT:
            post_data = encode_multipart(BOUNDARY, data)
        else:
            # Encode the content so that the byte representation is correct.
            match = CONTENT_TYPE_RE.match(content_type)
            if match:
                charset = match.group(1)
            else:
                charset = settings.DEFAULT_CHARSET
            post_data = smart_str(data, encoding=charset)

        parsed = urlparse(path)
        r = {
            'CONTENT_LENGTH': len(post_data),
            'CONTENT_TYPE':   content_type,
            'PATH_INFO':      unquote(parsed[2]),
            'QUERY_STRING':   parsed[4],
            'REQUEST_METHOD': 'POST',
            'wsgi.input':     fake_input(post_data),
        }
        r.update(extra)
        return self.request(**r)
    
    def head(self, path, data={}, **extra):
        "Construct a HEAD request."

        parsed = urlparse(path)
        r = {
            'CONTENT_TYPE':    'text/html; charset=utf-8',
            'PATH_INFO':       unquote(parsed[2]),
            'QUERY_STRING':    urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'HEAD',
            'wsgi.input':      fake_input()
        }
        r.update(extra)
        return self.request(**r)

    def options(self, path, data={}, **extra):
        "Constrict an OPTIONS request"

        parsed = urlparse(path)
        r = {
            'PATH_INFO':       unquote(parsed[2]),
            'QUERY_STRING':    urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'OPTIONS',
            'wsgi.input':      fake_input()
        }
        r.update(extra)
        return self.request(**r)

    def put(self, path, data={}, content_type=MULTIPART_CONTENT, **extra):
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
            'wsgi.input':     fake_input(post_data),
        }
        r.update(extra)
        return self.request(**r)

    def delete(self, path, data={}, **extra):
        "Construct a DELETE request."
        parsed = urlparse(path)
        r = {
            'PATH_INFO':       unquote(parsed[2]),
            'QUERY_STRING':    urlencode(data, doseq=True) or parsed[4],
            'REQUEST_METHOD': 'DELETE',
            'wsgi.input':      fake_input()
        }
        r.update(extra)
        return self.request(**r)