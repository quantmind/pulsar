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
import io
from asyncio import wait_for
from wsgiref.handlers import format_date_time

import pulsar
from pulsar import (reraise, HttpException, ProtocolError, Future, task,
                    isfuture, chain_future)
from pulsar.utils.pep import native_str
from pulsar.utils.httpurl import (Headers, unquote, has_empty_content,
                                  host_and_port_default, http_parser,
                                  urlparse, iri_to_uri, DEFAULT_CHARSET)

from pulsar.utils.internet import format_address, is_tls
from pulsar.async.protocols import ProtocolConsumer

from .utils import (handle_wsgi_error, wsgi_request, HOP_HEADERS,
                    log_wsgi_info, LOGGER)

__all__ = ['HttpServerResponse', 'MAX_CHUNK_SIZE', 'test_wsgi_environ']


MAX_CHUNK_SIZE = 65536
MAX_TIME_IN_LOOP = 0.5


def test_wsgi_environ(path=None, method=None, headers=None, extra=None,
                      secure=False, loop=None, body=None):
    '''An function to create a WSGI environment dictionary for testing.

    :param url: the resource in the ``PATH_INFO``.
    :param method: the ``REQUEST_METHOD``.
    :param headers: optional request headers
    :params secure: a secure connection?
    :param extra: additional dictionary of parameters to add.
    :return: a valid WSGI environ dictionary.
    '''
    parser = http_parser(kind=0)
    method = (method or 'GET').upper()
    path = iri_to_uri(path or '/')
    request_headers = Headers(headers, kind='client')
    # Add Host if not available
    parsed = urlparse(path)
    if 'host' not in request_headers:
        if not parsed.netloc:
            path = '%s%s' % ('https://:443' if secure else 'http://:80', path)
        else:
            request_headers['host'] = parsed.netloc
    #
    data = '%s %s HTTP/1.1\r\n\r\n' % (method, path)
    data = data.encode('latin1')
    parser.execute(data, len(data))
    #
    stream = io.BytesIO(body or b'')
    return wsgi_environ(stream, parser, request_headers,
                        ('127.0.0.1', 8060), '255.0.1.2:8080',
                        Headers(), https=secure, extra=extra)


def http_protocol(parser):
    version = parser.get_version()
    return "HTTP/%s" % ".".join(('%s' % v for v in version))


class StreamReader:
    _expect_sent = None
    _waiting = None

    def __init__(self, headers, parser, transport=None):
        self.headers = headers
        self.parser = parser
        self.transport = transport
        self.buffer = b''
        self.on_message_complete = Future()

    def __repr__(self):
        return repr(self.transport)
    __str__ = __repr__

    def done(self):
        '''``True`` when the full HTTP message has been read.
        '''
        return self.on_message_complete.done()

    def waiting_expect(self):
        '''``True`` when the client is waiting for 100 Continue.
        '''
        if self._expect_sent is None:
            if (not self.parser.is_message_complete() and
                    self.headers.has('expect', '100-continue')):
                return True
            self._expect_sent = ''
        return False

    def recv(self):
        '''Read bytes in the buffer.
        '''
        if self.waiting_expect():
            if self.parser.get_version() < (1, 1):
                raise HttpException(status=417)
            else:
                msg = '%s 100 Continue\r\n\r\n' % http_protocol(self.parser)
                self._expect_sent = msg
                self.transport.write(msg.encode(DEFAULT_CHARSET))
        return self.parser.recv_body()

    def read(self, maxbuf=None):
        '''Return bytes in the buffer.

        If the stream is not yet ready, return a :class:`asyncio.Future`
        which results in the bytes read.
        '''
        if not self._waiting:
            body = self.recv()
            if self.done():
                return self._getvalue(body, maxbuf)
            else:
                self._waiting = chain_future(
                    self.on_message_complete,
                    lambda r: self._getvalue(body, maxbuf))
                return self._waiting
        else:
            return self._waiting

    def fail(self):
        if self.waiting_expect():
            raise HttpException(status=417)

    #    INTERNALS
    def _getvalue(self, body, maxbuf):
        if self.buffer:
            body = self.buffer + body
        body = body + self.recv()
        if maxbuf and len(body) > maxbuf:
            body, self.buffer = body[:maxbuf], body[maxbuf:]
        return body


def wsgi_environ(stream, parser, request_headers, address, client_address,
                 headers, server_software=None, https=False, extra=None):
    '''Build the WSGI Environment dictionary

    :param stream: a wsgi stream object
    :param parser: pulsar HTTP parser
    :param request_headers: headers of request
    :param address: server address
    :param client_address: client address
    :param headers: container for response headers
    '''
    protocol = http_protocol(parser)
    raw_uri = parser.get_url()
    request_uri = urlparse(raw_uri)
    #
    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5.2
    # If Request-URI is an absoluteURI, the host is part of the Request-URI.
    # Any Host header field value in the request MUST be ignored
    if request_uri.scheme:
        url_scheme = request_uri.scheme
        host = request_uri.netloc
    else:
        url_scheme = 'https' if https else 'http'
        host = None
    #
    environ = {"wsgi.input": stream,
               "wsgi.errors": sys.stderr,
               "wsgi.version": (1, 0),
               "wsgi.run_once": False,
               "wsgi.multithread": False,
               "wsgi.multiprocess": False,
               "SERVER_SOFTWARE": server_software or pulsar.SERVER_SOFTWARE,
               "REQUEST_METHOD": native_str(parser.get_method()),
               "QUERY_STRING": parser.get_query_string(),
               "RAW_URI": raw_uri,
               "SERVER_PROTOCOL": protocol,
               "CONTENT_TYPE": ''}
    forward = client_address
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
        elif header == "host" and not host:
            host = value
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
    if url_scheme == 'https':
        environ['HTTPS'] = 'on'
    if isinstance(forward, str):
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
    if not host and protocol == 'HTTP/1.0':
        host = format_address(address)
    if host:
        h = host_and_port_default(url_scheme, host)
        environ['SERVER_NAME'] = socket.getfqdn(h[0]) if h[0] else '0.0.0.0'
        environ['SERVER_PORT'] = h[1]
    path_info = request_uri.path
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
        headers['connection'] = 'upgrade'
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
    '''Server side WSGI :class:`.ProtocolConsumer`.

    .. attribute:: wsgi_callable

        The wsgi callable handling requests.
    '''
    _status = None
    _headers_sent = None
    _stream = None
    _buffer = None
    _logger = LOGGER
    SERVER_SOFTWARE = pulsar.SERVER_SOFTWARE
    ONE_TIME_EVENTS = ProtocolConsumer.ONE_TIME_EVENTS + ('on_headers',)

    def __init__(self, wsgi_callable, cfg, server_software=None, loop=None):
        super().__init__(loop=loop)
        self.wsgi_callable = wsgi_callable
        self.cfg = cfg
        self.parser = http_parser(kind=0)
        self.headers = Headers()
        self.keep_alive = False
        self.SERVER_SOFTWARE = server_software or self.SERVER_SOFTWARE

    @property
    def headers_sent(self):
        '''Available once the headers have been sent to the client.

        These are the bytes representing the first response line and
        the headers
        '''
        return self._headers_sent

    def data_received(self, data):
        '''Implements :meth:`~.ProtocolConsumer.data_received` method.

        Once we have a full HTTP message, build the wsgi ``environ`` and
        delegate the response to the :func:`wsgi_callable` function.
        '''
        parser = self.parser
        processed = parser.execute(data, len(data))
        if not self._stream and parser.is_headers_complete():
            headers = Headers(parser.get_headers(), kind='client')
            self._stream = StreamReader(headers, parser, self.transport)
            self._response(self.wsgi_environ())
        #
        if parser.is_message_complete():
            #
            # Stream has the whole body
            if not self._stream.on_message_complete.done():
                self._stream.on_message_complete.set_result(None)

            if processed < len(data):
                if not self._buffer:
                    self._buffer = data[processed:]
                    self.bind_event('post_request', self._new_request)
                else:
                    self._buffer += data[processed:]
        #
        elif processed < len(data):
            # This is a parsing error, the client must have sent
            # bogus data
            raise ProtocolError

    @property
    def status(self):
        return self._status

    @property
    def upgrade(self):
        return self.headers.get('upgrade')

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
        More precisely, it is a fatal error to call ``start_response`` without
        the ``exc_info`` argument if start_response has already been called
        within the current invocation of the application.

        :parameter status: an HTTP ``status`` string like ``200 OK`` or
            ``404 Not Found``.
        :parameter response_headers: a list of ``(header_name, header_value)``
            tuples. It must be a Python list. Each header_name must be a valid
            HTTP header field-name (as defined by RFC 2616_, Section 4.2),
            without a trailing colon or other punctuation.
        :parameter exc_info: optional python ``sys.exc_info()`` tuple.
            This argument should be supplied by the application only if
            ``start_response`` is being called by an error handler.
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
                    reraise(*exc_info)
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
                self.logger.warning('Application passing hop header "%s"',
                                    header)
                continue
            self.headers.add_header(header, value)
        return self.write

    def write(self, data, force=False):
        '''The write function returned by the :meth:`start_response` method.

        Required by the WSGI specification.

        :param data: bytes to write
        :param force: Optional flag used internally
        :return: a :class:`~asyncio.Future` or the number of bytes written
        '''
        write = super().write
        chunks = []
        if not self._headers_sent:
            tosend = self.get_headers()
            self._headers_sent = tosend.flat(self.version, self.status)
            self.fire_event('on_headers')
            chunks.append(self._headers_sent)
        if data:
            if self.chunked:
                while len(data) >= MAX_CHUNK_SIZE:
                    chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
                    chunks.append(chunk_encoding(chunk))
                if data:
                    chunks.append(chunk_encoding(data))
            else:
                chunks.append(data)
        elif force and self.chunked:
            chunks.append(chunk_encoding(data))
        if chunks:
            return write(b''.join(chunks))

    ########################################################################
    #    INTERNALS
    @task
    def _response(self, environ):
        exc_info = None
        response = None
        done = False
        alive = self.cfg.keep_alive or 15
        while not done:
            done = True
            try:
                if exc_info is None:
                    if 'SERVER_NAME' not in environ:
                        raise HttpException(status=400)
                    response = self.wsgi_callable(environ, self.start_response)
                    if isfuture(response):
                        response = yield from wait_for(response, alive)
                else:
                    response = handle_wsgi_error(environ, exc_info)
                    if isfuture(response):
                        response = yield from wait_for(response, alive)
                #
                if exc_info:
                    self.start_response(response.status,
                                        response.get_headers(), exc_info)
                #
                # Do the actual writing
                loop = self._loop
                start = loop.time()
                for chunk in response:
                    if isfuture(chunk):
                        chunk = yield from wait_for(chunk, alive)
                        start = loop.time()
                    result = self.write(chunk)
                    if isfuture(result):
                        yield from wait_for(result, alive)
                        start = loop.time()
                    else:
                        time_in_loop = loop.time() - start
                        if time_in_loop > MAX_TIME_IN_LOOP:
                            self.logger.debug(
                                'Released the event loop after %.3f seconds',
                                time_in_loop)
                            yield None
                            start = loop.time()
                #
                # make sure we write headers and last chunk if needed
                self.write(b'', True)

            except IOError:     # client disconnected, end this connection
                self.finished()
            except Exception:
                if wsgi_request(environ).cache.handle_wsgi_error:
                    self.keep_alive = False
                    self._write_headers()
                    self.connection.close()
                    self.finished()
                else:
                    done = False
                    exc_info = sys.exc_info()
            else:
                if not self.keep_alive:
                    self.connection.close()
                self.finished()
                log_wsgi_info(self.logger.info, environ, self.status)
            finally:
                if hasattr(response, 'close'):
                    try:
                        response.close()
                    except Exception:
                        self.logger.exception(
                            'Error while closing wsgi iterator')

    def is_chunked(self):
        '''Check if the response uses chunked transfer encoding.

        Only use chunked responses when the client is speaking HTTP/1.1
        or newer and there was no Content-Length header set.
        '''
        if (self.version <= (1, 0) or
                self._status == '200 Connection established' or
                has_empty_content(int(self.status[:3]))):
            return False
        elif self.headers.get('Transfer-Encoding') == 'chunked':
            return True
        else:
            return self.content_length is None

    def get_headers(self):
        '''Get the headers to send to the client.
        '''
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
        # return a the WSGI environ dictionary
        transport = self.transport
        https = True if is_tls(transport.get_extra_info('socket')) else False
        multiprocess = (self.cfg.concurrency == 'process')
        environ = wsgi_environ(self._stream,
                               self.parser,
                               self._stream.headers,
                               transport.get_extra_info('sockname'),
                               self.address, self.headers,
                               self.SERVER_SOFTWARE,
                               https=https,
                               extra={'pulsar.connection': self.connection,
                                      'pulsar.cfg': self.cfg,
                                      'wsgi.multiprocess': multiprocess})
        self.keep_alive = keep_alive(self.headers, self.parser.get_version())
        self.headers.update([('Server', self.SERVER_SOFTWARE),
                             ('Date', format_date_time(time.time()))])
        return environ

    def _new_request(self, _, exc=None):
        connection = self._connection
        connection.data_received(self._buffer)

    def _write_headers(self):
        if not self._headers_sent:
            if self.content_length:
                self.headers['Content-Length'] = '0'
            self.write(b'')
