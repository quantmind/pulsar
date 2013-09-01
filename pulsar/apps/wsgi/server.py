'''
HTTP Protocol Consumer
==============================

.. autoclass:: HttpServerResponse
   :members:
   :member-order: bysource
   
   
Testing WSGI Environ
=========================

.. autofunction:: test_wsgi_environ
'''
import sys
import time
import os
import socket
from functools import partial
from wsgiref.handlers import format_date_time
from io import BytesIO

import pulsar
from pulsar import HttpException, ProtocolError, maybe_async, Deferred, Failure
from pulsar.utils.pep import is_string, native_str, raise_error_trace
from pulsar.utils.httpurl import (Headers, unquote, has_empty_content,
                                  host_and_port_default, http_parser)
                                 
from pulsar.utils.internet import format_address
from pulsar.async.protocols import ProtocolConsumer
from pulsar.utils import events

from .utils import handle_wsgi_error, LOGGER, HOP_HEADERS


__all__ = ['HttpServerResponse', 'MAX_CHUNK_SIZE', 'test_wsgi_environ']

MAX_CHUNK_SIZE = 65536


def test_wsgi_environ(url='/', method=None, headers=None, extra=None):
    '''An utility function for creating a WSGI environment dictionary
for testing purposes.
    
:param url: the resource in the ``PATH_INFO``.
:param method: the ``REQUEST_METHOD``.
:param headers: optional request headers
:param extra: optional dictionary of additional key-valued parameters to add.
:return: a valid WSGI environ dictionary.
'''
    parser = http_parser(kind=0)
    method = (method or 'GET').upper()
    data = '%s %s HTTP/1.1\r\n\r\n' % (method, url)
    data = data.encode('utf-8')
    parser.execute(data, len(data))
    request_headers = headers or []
    headers = Headers()
    return wsgi_environ(parser, ('127.0.0.1', 8060), '777.777.777.777:8080',
                        request_headers, headers, extra)
    
    
def wsgi_environ(parser, address, client_address, request_headers,
                 headers, extra=None):
    protocol = "HTTP/%s" % ".".join(('%s' % v for v in parser.get_version()))
    environ = {
            "wsgi.input": BytesIO(parser.recv_body()),
            "wsgi.errors": sys.stderr,
            "wsgi.version": (1, 0),
            "wsgi.run_once": False,
            'wsgi.multithread': False,
            'wsgi.multiprocess': False,
            "SERVER_SOFTWARE": pulsar.SERVER_SOFTWARE,
            "REQUEST_METHOD": native_str(parser.get_method()),
            "QUERY_STRING": parser.get_query_string(),
            "RAW_URI": parser.get_url(),
            "SERVER_PROTOCOL": protocol,
            'CONTENT_TYPE': ''
        }
    url_scheme = "http"
    forward = client_address
    server = format_address(address)
    script_name = os.environ.get("SCRIPT_NAME", "")
    for header, value in request_headers:
        header = header.lower()
        if header in HOP_HEADERS:
            headers[header] = value
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
    server =  host_and_port_default(url_scheme, server)
    environ['SERVER_NAME'] = socket.getfqdn(server[0])
    environ['SERVER_PORT'] = server[1]
    path_info = parser.get_path()
    if path_info is not None:
        if script_name:
            path_info = path_info.split(script_name, 1)[1]
        environ['PATH_INFO'] = unquote(path_info)
    environ['SCRIPT_NAME'] = script_name
    if extra:
        environ.update(extra)
    return environ
    
    
def chunk_encoding(chunk):
    '''Write a chunk::

    chunk-size(hex) CRLF
    chunk-data CRLF
    
If the size is 0, this is the last chunk, and an extra CRLF is appended.
'''
    head = ("%X\r\n" % len(chunk)).encode('utf-8')
    return head + chunk + b'\r\n'

def keep_alive(headers, version):
        """ return True if the connection should be kept alive"""
        conn = set((v.lower() for v in headers.get_all('connection', ())))
        if "close" in conn:
            return False
        elif 'upgrade' in conn:
            headers['connection'] = 'Upgrade'
            return True
        elif "keep-alive" in conn:
            return True
        elif version == (1, 1):
            headers['connection'] = 'keep-alive'
            return True
        else:
            return False
        
def keep_alive_with_status(status, headers):
    code = int(status.split()[0])
    if code >= 400:
        return False
    return True
    

class HttpServerResponse(ProtocolConsumer):
    '''Server side HTTP :class:`pulsar.ProtocolConsumer`.'''
    _status = None
    _headers_sent = None
    _request_headers = None
    max_empty_consecutive = 20
    SERVER_SOFTWARE = pulsar.SERVER_SOFTWARE
    
    def __init__(self, wsgi_callable, cfg, connection):
        super(HttpServerResponse, self).__init__(connection)
        self.wsgi_callable = wsgi_callable
        self.cfg = cfg
        self.parser = http_parser(kind=0)
        self.headers = Headers()
        self.keep_alive = False
        
    def data_received(self, data):
        '''Implements :class:`pulsar.Protocol.data_received`. Once we have a
full HTTP message, build the wsgi ``environ`` and write the response
using the :meth:`pulsar.Transport.writelines` method.'''
        p = self.parser
        request_headers = self._request_headers
        if p.execute(bytes(data), len(data)) == len(data):
            done = p.is_message_complete()
            if request_headers is None and p.is_headers_complete():
                self._request_headers = Headers(p.get_headers(), kind='client')
                if not done:
                    self.expect_continue()
            if done: # message is done
                self.generate(self.wsgi_environ())
        else:
            # This is a parsing error, the client must have sent
            # bogus data
            raise ProtocolError
    
    def expect_continue(self):
        '''Handle the expect=100-continue header if available, according to
the following algorithm:

* Send the 100 Continue response before waiting for the body.
* Omit the 100 (Continue) response if it has already received some or all of
  the request body for the corresponding request.
    '''
        if self._request_headers.has('expect', '100-continue'):
            self.transport.write(b'HTTP/1.1 100 Continue\r\n\r\n')
    
    @property
    def status(self):
        return self._status

    @property
    def upgrade(self):
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
        return self.parser.get_version()

    def start_response(self, status, response_headers, exc_info=None):
        '''WSGI compliant ``start_response`` callable, see pep3333_.
The application may call start_response more than once, if and only
if the ``exc_info`` argument is provided.
More precisely, it is a fatal error to call ``start_response`` without the
``exc_info`` argument if start_response has already been called within
the current invocation of the application.

:parameter status: an HTTP ``status`` string like ``200 OK`` or
    ``404 Not Found``.
:parameter response_headers: a list of ``(header_name, header_value)`` tuples.
    It must be a Python list. Each header_name must be a valid HTTP header
    field-name (as defined by RFC 2616_, Section 4.2), without a trailing
    colon or other punctuation.
:parameter exc_info: optional python ``sys.exc_info()`` tuple. This argument
    should be supplied by the application only if start_response is being
    called by an error handler.
:return: The :meth:`write` method.

``HOP_HEADERS`` are not considered but no error is raised.

.. _pep3333: http://www.python.org/dev/peps/pep-3333/
.. _2616: http://www.faqs.org/rfcs/rfc2616.html
'''
        if exc_info:
            try:
                if self._headers_sent:
                    # if exc_info is provided, and the HTTP headers have
                    # already been sent, start_response must raise an error,
                    # and should re-raise using the exc_info tuple
                    raise_error_trace(exc_info[1], exc_info[2])
            finally:
                # Avoid circular reference
                exc_info = None
        elif self._status:
            # Headers already set. Raise error
            raise HttpException("Response headers already set!")
        self._status = status
        if type(response_headers) is not list:
            raise TypeError("Headers must be a list of name/value tuples")
        for header, value in response_headers:
            if header.lower() in HOP_HEADERS:
                # These features are the exclusive province of this class,
                # this should be considered a fatal error for an application
                # to attempt sending them, but we don't raise an error,
                # just log a warning
                LOGGER.warning('Application handler passing hop header "%s"',
                               header)
                continue
            self.headers.add_header(header, value)
        return self.write

    def write(self, data, force=False):
        '''The write function which is returned by the :meth:`start_response`
method as required by the WSGI specification.

:param data: bytes to write
:param force: Optional flag used internally.
'''
        if not self._headers_sent:
            tosend = self.get_headers()
            events.fire('http-headers', self, headers=tosend)
            self._headers_sent = tosend.flat(self.version, self.status)
            self.transport.write(self._headers_sent)
        if data:
            if self.chunked:
                while len(data) >= MAX_CHUNK_SIZE:
                    chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
                    self.transport.write(chunk_encoding(chunk))
                if data:
                    self.transport.write(chunk_encoding(data))
            else:
                self.transport.write(data)
        elif force and self.chunked:
            self.transport.write(chunk_encoding(data))

    def generate(self, environ, failure=None):
        try:
            if failure:
                # A failure has occurred, try to handle it with the default
                # wsgi error handler
                wsgi_iter = self.handle_wsgi_error(environ, failure)
            else:
                wsgi_iter = self.wsgi_callable(environ, self.start_response)
        except Exception:
            result = sys.exc_info()
        else:
            if not isinstance(wsgi_iter, Failure):
                result = self.async_wsgi(wsgi_iter)
            else:
                result = wsgi_iter
        result = maybe_async(result, get_result=False)
        err_handler = self.generate if failure is None else self.catastrofic
        result.add_errback(partial(err_handler, environ))
        
    def async_wsgi(self, wsgi_iter):
        '''Asynchronous WSGI server handler. Fully conforms with `WSGI 1.0.1`_
when the ``wsgi_iter`` yields bytes only.'''
        if isinstance(wsgi_iter, Deferred): # handle asynchronous wsgi response
            wsgi_iter = yield wsgi_iter 
        try:
            for b in wsgi_iter:
                chunk = yield b # handle asynchronous components
                self.write(chunk)
            # make sure we write headers
            self.write(b'', True)
        finally:
            if hasattr(wsgi_iter, 'close'):
                try:
                    wsgi_iter.close()
                except Exception:
                    LOGGER.exception('Error while closing wsgi iterator')
        self.finish_wsgi()
        
    def handle_wsgi_error(self, environ, failure):
        exc_info = failure.exc_info
        response = handle_wsgi_error(environ, failure)
        self.start_response(response.status, response.get_headers(), exc_info)
        return response
        
    def catastrofic(self, environ, failure):
        self.keep_alive = False
        self.finish_wsgi()
        
    def finish_wsgi(self):
        if not self.keep_alive:
            self.connection.close()
        self.finished()
            
    def is_chunked(self):
        '''Only use chunked responses when the client is
speaking HTTP/1.1 or newer and there was no Content-Length header set.'''
        if self.version <= (1, 0):
            return False
        elif has_empty_content(int(self.status[:3])):
            # Do not use chunked responses when the response
            # is guaranteed to not have a response body.
            return False
        elif self.headers.get('Transfer-Encoding') == 'chunked':
            return True
        else:
            return self.content_length is None

    def get_headers(self):
        '''Get the headers to send only if *force* is ``True`` or this
is an HTTP upgrade (websockets)'''
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
        if self.keep_alive:
            self.keep_alive = keep_alive_with_status(self._status, headers)
        if not self.keep_alive:
            headers['connection'] = 'close'
        return headers

    def wsgi_environ(self):
        #return a the WSGI environ dictionary
        parser = self.parser
        environ = wsgi_environ(parser, self.transport.address, self.address,
                               self._request_headers, self.headers,
                               {'pulsar.connection': self.connection,
                                'pulsar.cfg': self.cfg})
        self.keep_alive = keep_alive(self.headers, parser.get_version())
        self.headers.update([('Server', self.SERVER_SOFTWARE),
                             ('Date', format_date_time(time.time()))])
        return environ