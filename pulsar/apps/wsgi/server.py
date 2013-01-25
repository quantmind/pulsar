import sys
import time
import os
import socket
from wsgiref.handlers import format_date_time
from io import BytesIO

import pulsar
from pulsar import lib, HttpException
from pulsar.utils.pep import is_string, to_bytes
from pulsar.utils.httpurl import Headers, unquote, has_empty_content,\
                                    host_and_port_default, mapping_iterator,\
                                    Headers, REDIRECT_CODES
from pulsar.utils import events

from .wsgi import WsgiResponse, handle_wsgi_error, LOGGER


__all__ = ['wsgi_environ', 'HttpServerResponse', 'MAX_CHUNK_SIZE']

MAX_CHUNK_SIZE = 65536


def wsgi_environ(response, parser):
    """return a :ref:`WSGI <apps-wsgi>` compatible environ dictionary
based on the current request. If the reqi=uest headers are not ready it returns
nothing."""
    version = parser.get_version()
    input = BytesIO()
    for b in parser.get_body():
        input.write(b)
    input.seek(0)
    prot = parser.get_protocol()
    environ = {
        "wsgi.input": input,
        "wsgi.errors": sys.stderr,
        "wsgi.version": version,
        "wsgi.run_once": True,
        "wsgi.url_scheme": prot,
        "SERVER_SOFTWARE": pulsar.SERVER_SOFTWARE,
        "REQUEST_METHOD": parser.get_method(),
        "QUERY_STRING": parser.get_query_string(),
        "RAW_URI": parser.get_url(),
        "SERVER_PROTOCOL": prot,
        'CONTENT_TYPE': '',
        "CONTENT_LENGTH": '',
        'SERVER_NAME': response.server_name,
        'SERVER_PORT': response.server_port,
        "wsgi.multithread": False,
        "wsgi.multiprocess":False
    }
    # REMOTE_HOST and REMOTE_ADDR may not qualify the remote addr:
    # http://www.ietf.org/rfc/rfc3875
    url_scheme = "http"
    forward = response.address
    server = None
    url_scheme = "http"
    script_name = os.environ.get("SCRIPT_NAME", "")
    headers = mapping_iterator(parser.get_headers())
    for header, value in headers:
        header = header.lower()
        if header == 'x-forwarded-for':
            forward = value
        elif header == "x-forwarded-protocol" and value == "ssl":
            url_scheme = "https"
        elif header == "x-forwarded-ssl" and value == "on":
            url_scheme = "https"
        elif header == "host":
            server = value
        elif header == "script_name":
            script_name = value
        elif header == "content-type":
            environ['CONTENT_TYPE'] = value
            continue
        elif header == "content-length":
            environ['CONTENT_LENGTH'] = value
            continue
        key = 'HTTP_' + header.upper().replace('-', '_')
        environ[key] = value
    environ['wsgi.url_scheme'] = url_scheme
    if is_string(forward):
        # we only took the last one
        # http://en.wikipedia.org/wiki/X-Forwarded-For
        if forward.find(",") >= 0:
            forward = forward.rsplit(",", 1)[1].strip()
        remote = forward.split(":")
        if len(remote) < 2:
            remote.append('80')
    else:
        remote = forward
    environ['REMOTE_ADDR'] = remote[0]
    environ['REMOTE_PORT'] = str(remote[1])
    if server is not None:
        server =  host_and_port_default(url_scheme, server)
        environ['SERVER_NAME'] = server[0]
        environ['SERVER_PORT'] = server[1]
    path_info = parser.get_path()
    if path_info is not None:
        if script_name:
            path_info = path_info.split(script_name, 1)[1]
        environ['PATH_INFO'] = unquote(path_info)
    environ['SCRIPT_NAME'] = script_name
    return environ

def chunk_encoding(chunk):
    '''Write a chunk::

    chunk-size(hex) CRLF
    chunk-data CRLF
    
If the size is 0, this is the last chunk, and an extra CRLF is appended.
'''
    head = ("%X\r\n" % len(chunk)).encode('utf-8')
    return head + chunk + b'\r\n'


class HttpServerResponse(pulsar.ProtocolConsumer):
    '''Handle an HTTP response for a :class:`HttpConnection`.'''
    _status = None
    _headers_sent = None
    
    def __init__(self, wsgi_callable, connection):
        super(HttpServerResponse, self).__init__(connection)
        self.wsgi_callable = wsgi_callable
        host, port = self.sock.address[:2]
        self.server_name = socket.getfqdn(host)
        self.server_port = port
        self.parser = lib.Http_Parser(kind=0)
        
    def data_received(self, data):
        # Got data from the transport, lets parse it
        p = self.parser
        request_headers = self.request_headers
        if p.execute(bytes(data), len(data)) == len(data):
            if request_headers is None:
                self.expect_continue()
            if p.is_message_complete(): # message is done
                self.environ = wsgi_environ(self, p)
                self.writelines(self.generate(self.environ))
        else:
            # This is a parsing error, the client must have sent
            # bogus data
            raise ProtocolError
    
    def default_headers(self):
        return Headers([('Server', pulsar.SERVER_SOFTWARE),
                        ('Date', format_date_time(time.time()))])
    
    @property
    def request_headers(self):
        if self.parser.is_headers_complete():
            return self.parser.get_headers()
        
    def expect_continue(self):
        '''Handle the expect=100-continue header if available, according to
the following algorithm:

* Send the 100 Continue response before waiting for the body.
* Omit the 100 (Continue) response if it has already received some or all of
  the request body for the corresponding request.
    '''
        headers = self.request_headers
        if headers is not None and headers.get('Expect') == '100-continue':
            if not self.parser.is_message_complete():
                self.protocol.write(b'HTTP/1.1 100 Continue\r\n\r\n')
    
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
            raise HttpException("Response headers already sent!")
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
            self.protocol.write(head)
        if data:
            self.protocol.write(data)

    def generate(self, environ):
        exc_info = None
        keep_alive = self.keep_alive
        # Inject upgrade_protocol into the environment
        # TODO: is this the best way to do it?
        environ['upgrade_protocol'] = self.connection.upgrade
        wsgi = lambda e, s, err: self.wsgi_callable(e,s)
        while True:
            try:
                for b in wsgi(environ, self.start_response, exc_info):
                    head = self.send_headers(force=b)
                    if head is not None:
                        yield head
                    if b:
                        if self.chunked:
                            while len(b) >= MAX_CHUNK_SIZE:
                                chunk, b = b[:MAX_CHUNK_SIZE], b[MAX_CHUNK_SIZE:]
                                yield chunk_encoding(chunk)
                            if b:
                                yield chunk_encoding(b)
                        else:
                            yield b
                    else:
                        yield b''
            except Exception as e:
                if exc_info or self._headers_sent:
                    keep_alive = False
                    LOGGER.critical('Could not send valid response',
                                    exc_info=True)
                    break
                else:
                    exc_info = sys.exc_info()
                    wsgi = handle_wsgi_error(environ, exc_info)
                    if keep_alive and wsgi.status_code in REDIRECT_CODES:
                        wsgi.headers['connection'] = 'keep-alive'
                    else:
                        keep_alive = False
                        wsgi.headers['connection'] = 'close'
            else:
                head = self.send_headers(force=True)
                if head is not None:
                    yield head
                if self.chunked:
                    # Last chunk
                    yield chunk_encoding(b'')
                break
        # close transport if required
        if not keep_alive:
            self.protocol.close()
        self.finished()

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

