import socket

from pulsar import lib


class HttpResponse(ProtocolResponse):
    '''Handle an HTTP response for a :class:`HttpConnection`.'''
    _status = None
    _headers_sent = None
    headers = None
    MAX_CHUNK = 65536
    
    def __init__(self, protocol):
        super(HttpResponse, self).__init__(protocol)
        host, port = self.sock.getsockname()[:2]
        self.server_name = socket.getfqdn(host)
        self.server_port = port
        self.parser = lib.Http_Parser(kind=0)
        
        
    def default_headers(self):
        return Headers([('Server', pulsar.SERVER_SOFTWARE),
                        ('Date', format_date_time(time.time()))])
        
    def feed(self, data):
        p = self.parser
        headers = self.headers()
        if p.execute(bytes(data), len(data)) == len(data):
            if headers is None:
                self.expect_continue()
            if p.is_message_complete():
                environ = wsgi_environ(self.connection, p)
    
    def expect_continue(self):
        '''Handle the expect=100-continue header if available, according to
the following algorithm:

* Send the 100 Continue response before waiting for the body.
* Omit the 100 (Continue) response if it has already received some or all of
  the request body for the corresponding request.
    '''
        headers = self.headers()
        if headers is not None and headers.get('Expect') == '100-continue':
            if not self.parser.is_message_complete():
                self.connection.write(b'HTTP/1.1 100 Continue\r\n\r\n')
        

    @property
    def environ(self):
        return self.parsed_data

    @property
    def status(self):
        return self._status

    @property
    def upgrade(self):
        if self.headers:
            return self.headers.get('Upgrade')

    @property
    def chunked(self):
        return self.headers.get('Transfer-Encoding') == 'chunked'

    @property
    def content_length(self):
        c = self.headers.get('Content-Length')
        if c:
            return int(c)

    @property
    def version(self):
        return self.environ.get('wsgi.version')

    @property
    def keep_alive(self):
        """ return True if the connection should be kept alive"""
        conn = self.environ.get('HTTP_CONNECTION','').lower()
        if conn == "close":
            return False
        elif conn == "keep-alive":
            return True
        return self.version == (1, 1)

    def start_response(self, status, response_headers, exc_info=None):
        '''WSGI compliant ``start_response`` callable, see pep3333_.
The application may call start_response more than once, if and only
if the exc_info argument is provided.
More precisely, it is a fatal error to call start_response without the exc_info
argument if start_response has already been called within the current
invocation of the application.

:parameter status: an HTTP "status" string like "200 OK" or "404 Not Found".
:parameter response_headers: a list of ``(header_name, header_value)`` tuples.
    It must be a Python list. Each header_name must be a valid HTTP header
    field-name (as defined by RFC 2616_, Section 4.2), without a trailing
    colon or other punctuation.
:parameter exc_info: optional python ``sys.exc_info()`` tuple. This argument
    should be supplied by the application only if start_response is being
    called by an error handler.

:rtype: The :meth:`HttpResponse.write` callable.

.. _pep3333: http://www.python.org/dev/peps/pep-3333/
.. _2616: http://www.faqs.org/rfcs/rfc2616.html
'''
        if exc_info:
            try:
                if self._headers_sent:
                    # if exc_info is provided, and the HTTP headers have
                    # already been sent, start_response must raise an error,
                    # and should re-raise using the exc_info tuple
                    raise (exc_info[0], exc_info[1], exc_info[2])
            finally:
                # Avoid circular reference
                exc_info = None
        elif self._status:
            # Headers already sent. Raise error
            raise pulsar.HttpException("Response headers already sent!")
        self._status = status
        if type(response_headers) is not list:
            raise TypeError("Headers must be a list of name/value tuples")
        self.headers = self.default_headers()
        self.headers.update(response_headers)
        return self.write

    def write(self, data):
        '''The write function required by WSGI specification.'''
        head = self.send_headers(force=data)
        if head:
            self.connection.write(head)
        if data:
            self.connection.write(data)

    def _generate(self, response):
        MAX_CHUNK = self.MAX_CHUNK
        for b in response:
            head = self.send_headers(force=b)
            if head is not None:
                yield head
            if b:
                if self.chunked:
                    while len(b) >= MAX_CHUNK:
                        chunk, b = b[:MAX_CHUNK], b[MAX_CHUNK:]
                        yield chunk_encoding(chunk)
                    if b:
                        yield chunk_encoding(b)
                else:
                    yield b
            else:
                yield b''
            
    def __iter__(self):
        conn = self.connection
        keep_alive = self.keep_alive
        self.environ['pulsar.connection'] = self.connection
        try:
            resp = conn.server.app_handler(self.environ, self.start_response)
            for b in self._generate(resp):
                yield b
        except Exception as e:
            exc_info = sys.exc_info()
            if self._headers_sent:
                keep_alive = False
                conn.log.critical('Headers already sent', exc_info=exc_info)
                yield b'CRITICAL SERVER ERROR. Please Contact the administrator'
            else:
                # Create the error response
                resp = handle_wsgi_error(self.environ, exc_info)
                keep_alive = keep_alive and resp.status_code in REDIRECT_CODES
                resp.headers['connection'] =\
                    "keep-alive" if keep_alive else "close"
                resp(self.environ, self.start_response, exc_info)
                for b in self._generate(resp):
                    yield b
        # make sure we send the headers
        head = self.send_headers(force=True)
        if head is not None:
            yield head
        if self.chunked:
            # Last chunk
            yield chunk_encoding(b'')
        # close connection if required
        if not keep_alive:
            self.connection.close()

    def is_chunked(self):
        '''Only use chunked responses when the client is
speaking HTTP/1.1 or newer and there was no Content-Length header set.'''
        if self.environ['wsgi.version'] <= (1,0):
            return False
        elif has_empty_content(int(self.status[:3])):
            # Do not use chunked responses when the response
            # is guaranteed to not have a response body.
            return False
        elif self.headers.get('Transfer-Encoding') == 'chunked':
            return True
        else:
            return self.content_length is None

    def get_headers(self, force=False):
        '''Get the headers to send only if *force* is ``True`` or this
is an HTTP upgrade (websockets)'''
        if self.upgrade or force:
            if not self._status:
                # we are sending headers but the start_response was not called
                raise HttpException('Headers not set.')
            headers = self.headers
            # Set chunked header if needed
            if self.is_chunked():
                headers['Transfer-Encoding'] = 'chunked'
                headers.pop('content-length', None)
            else:
                headers.pop('Transfer-Encoding', None)
            if 'connection' not in headers:
                connection = "keep-alive" if self.keep_alive else "close"
                headers['Connection'] = connection
            return headers

    def send_headers(self, force=False):
        if not self._headers_sent:
            tosend = self.get_headers(force)
            if tosend:
                events.fire('http-headers', self, headers=tosend)
                self._headers_sent = tosend.flat(self.version, self.status)
                return self._headers_sent

    def close(self, msg=None):
        self.connection._current_response = None
        if not self.keep_alive:
            return self.connection.close(msg)


class HttpServer(pulsar.TCPServer):
    response_class = HttpResponse
    protocol_factory = HttpProtocol
    

