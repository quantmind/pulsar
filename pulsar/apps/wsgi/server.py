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
import io
from asyncio import sleep
from urllib.parse import urlparse, unquote

from async_timeout import timeout
from multidict import CIMultiDict, istr

import pulsar
from pulsar import reraise, HttpException, isawaitable, BadRequest
from pulsar.utils.httpurl import (has_empty_content, http_parser,
                                  iri_to_uri, http_chunks, http_date,
                                  http_headers_message)
from pulsar.async.protocols import ProtocolConsumer
from pulsar.utils import http

from .utils import (handle_wsgi_error, wsgi_request,
                    log_wsgi_info, LOGGER, get_logger, server_name)
from .formdata import HttpBodyReader
from .wrappers import FileWrapper, close_object
from .headers import (
    DATE, SERVER, CONNECTION, TRANSFER_ENCODING, CONTENT_LENGTH,
    X_FORWARDED_FOR, HOP_HEADERS, HEADER_WSGI, no_hop
)
try:
    from pulsar.utils.lib import http_write
except ImportError:
    http_write = None


CHARSET = http.CHARSET
MAX_TIME_IN_LOOP = 0.3
HTTP_1_1 = (1, 1)
URL_SCHEME = os.environ.get('wsgi.url_scheme', 'http')
ENVIRON = {
    "wsgi.errors": sys.stderr,
    "wsgi.file_wrapper": FileWrapper,
    "wsgi.version": (1, 0),
    "wsgi.run_once": False,
    "wsgi.multithread": True,
    "wsgi.multiprocess": True,
    "SCRIPT_NAME": os.environ.get("SCRIPT_NAME", ""),
    "CONTENT_TYPE": ''
}


class AbortWsgi(Exception):
    pass


def test_wsgi_environ(path=None, method=None, headers=None, extra=None,
                      https=False, loop=None, body=None, **params):
    '''An function to create a WSGI environment dictionary for testing.

    :param url: the resource in the ``PATH_INFO``.
    :param method: the ``REQUEST_METHOD``.
    :param headers: optional request headers
    :params https: a secure connection?
    :param extra: additional dictionary of parameters to add to ``params``
    :param params: key valued parameters
    :return: a valid WSGI environ dictionary.
    '''
    parser = http_parser(kind=0)
    method = (method or 'GET').upper()
    path = iri_to_uri(path or '/')
    request_headers = CIMultiDict(headers)
    # Add Host if not available
    parsed = urlparse(path)
    if 'host' not in request_headers:
        if not parsed.netloc:
            scheme = ('https' if https else 'http')
            path = '%s://127.0.0.1%s' % (scheme, path)
        else:
            request_headers['host'] = parsed.netloc
    #
    data = '%s %s HTTP/1.1\r\n\r\n' % (method, path)
    data = data.encode('latin1')
    parser.execute(data, len(data))
    #
    stream = io.BytesIO(body or b'')
    if extra:
        params.update(extra)
    return wsgi_environ(stream, parser, request_headers,
                        ('127.0.0.1', 8060), '255.0.1.2:8080',
                        CIMultiDict(), https=https, extra=params)


def wsgi_environ(stream, version, raw_uri, method,
                 request_headers, address,
                 client_address, headers, server_software=None,
                 https=False, extra=None):
    '''Build the WSGI Environment dictionary

    :param stream: a wsgi stream object
    :param parser: pulsar HTTP parser
    :param request_headers: headers of request
    :param address: server address
    :param client_address: client address
    :param headers: container for response headers
    '''
    protocol = "HTTP/%s" % version
    request_uri = http.parse_url(raw_uri)
    query = request_uri.query or b''
    url_scheme = request_uri.schema
    #
    if url_scheme:
        url_scheme = url_scheme.decode(CHARSET)
    else:
        url_scheme = 'https' if https else URL_SCHEME
    #
    environ = ENVIRON.copy()
    environ.update({
        "wsgi.input": stream,
        'wsgi.url_scheme': url_scheme,
        "SERVER_SOFTWARE": server_software or pulsar.SERVER_SOFTWARE,
        "REQUEST_METHOD": method,
        "QUERY_STRING": query.decode(CHARSET),
        "RAW_URI": raw_uri.decode(CHARSET),
        "SERVER_PROTOCOL": protocol,
        "CONTENT_TYPE": ''
    })
    for header, value in request_headers.items():
        if header in HOP_HEADERS:
            headers[header] = value
        else:
            hnd = HEADER_WSGI.get(header)
            if hnd and hnd(environ, value):
                continue
        key = 'HTTP_%s' % header.upper().replace('-', '_')
        environ[key] = value

    if environ['wsgi.url_scheme'] == 'https':
        environ['HTTPS'] = 'on'
    forward = headers.get(X_FORWARDED_FOR)
    if isinstance(forward, str):
        # we only took the last one
        # http://en.wikipedia.org/wiki/X-Forwarded-For
        if forward.find(",") >= 0:
            forward = forward.rsplit(",", 1)[1].strip()
        remote = forward.split(":")
        if len(remote) < 2:
            remote.append('80')
    else:
        remote = client_address
    environ['REMOTE_ADDR'] = remote[0]
    environ['REMOTE_PORT'] = str(remote[1])
    environ['SERVER_NAME'] = server_name(address[0])
    environ['SERVER_PORT'] = address[1]
    path_info = request_uri.path
    if path_info is not None:
        path_info = path_info.decode(CHARSET)
        script_name = environ.get('SCRIPT_NAME')
        if script_name:
            path_info = path_info.split(script_name, 1)[1]
        environ['PATH_INFO'] = unquote(path_info)
    if extra:
        environ.update(extra)
    return environ


def keep_alive(headers, version, method):
    """ return True if the connection should be kept alive"""
    conn = set(headers.getall(CONNECTION, ()))
    if "close" in conn:
        return False
    elif 'upgrade' in conn:
        headers[CONNECTION] = 'upgrade'
        return True
    elif "keep-alive" in conn:
        if version == HTTP_1_1:
            headers.pop(CONNECTION)
        return True
    elif version == HTTP_1_1:
        return True
    elif method == 'CONNECT':
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
    _buffer = None
    logger = LOGGER
    SERVER_SOFTWARE = pulsar.SERVER_SOFTWARE
    ONE_TIME_EVENTS = ProtocolConsumer.ONE_TIME_EVENTS + ('on_headers',)

    def __init__(self, wsgi_callable, cfg, server_software=None, loop=None):
        self._loop = loop
        self.wsgi_callable = wsgi_callable
        self.cfg = cfg
        self.body_reader = HttpBodyReader()
        self.on_message_complete = self.body_reader.feed_eof
        self.parser = http.HttpRequestParser(self)
        self.keep_alive = False
        self.headers = CIMultiDict()
        self.data_received = self.parser.feed_data
        self.SERVER_SOFTWARE = server_software or self.SERVER_SOFTWARE

    @property
    def headers_sent(self):
        '''Available once the headers have been sent to the client.

        These are the bytes representing the first response line and
        the headers
        '''
        return self._headers_sent

    def on_url(self, url):
        self.raw_uri = url

    def on_header(self, name, value):
        name = istr(name.decode(CHARSET))
        #if HEADER_RE.search(name):
        #    raise InvalidHeader("invalid header name %s" % name)
        self.headers[name] = value.decode(CHARSET)

    def on_headers_complete(self):
        self.version = self.parser.get_http_version()
        self.method = self.parser.get_method().decode(CHARSET)
        self._loop.create_task(self._response())

    def on_body(self, body):
        if not self.body_reader.reader:
            self.body_reader.initialise(
                self.headers, self.parser, self.transport,
                self.cfg.stream_buffer, loop=self._loop
            )
        self.body_reader.feed_data(body)

    @property
    def status(self):
        return self._status

    @property
    def upgrade(self):
        return self.headers.get('upgrade')

    @property
    def chunked(self):
        return self.headers.get(TRANSFER_ENCODING) == 'chunked'

    @property
    def content_length(self):
        c = self.headers.get(CONTENT_LENGTH)
        if c:
            return int(c)

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

        self.headers.extend(no_hop(response_headers))
        if http_write:
            self.write = http_write(self).write
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
            self._headers_sent = http_headers_message(
                tosend, self.version, self.status
            )
            self.event('on_headers').fire()
            chunks.append(self._headers_sent)
        if data:
            if self.chunked:
                chunks.extend(http_chunks(data))
            else:
                chunks.append(data)
        elif force and self.chunked:
            chunks.extend(http_chunks(data, True))
        if chunks:
            return write(b''.join(chunks))

    ########################################################################
    #    INTERNALS
    async def _response(self):
        exc_info = None
        response = None
        done = False
        alive = self.cfg.keep_alive or 15
        environ = self.wsgi_environ()
        while not done:
            done = True
            try:
                with timeout(alive, loop=self._loop):
                    if exc_info is None:
                        if (not environ.get('HTTP_HOST') and
                                environ['SERVER_PROTOCOL'] != 'HTTP/1.0'):
                            raise BadRequest
                        response = self.wsgi_callable(environ,
                                                      self.start_response)
                        if isawaitable(response):
                            response = await response
                    else:
                        response = handle_wsgi_error(environ, exc_info)
                        if isawaitable(response):
                            response = await response
                    #
                    if exc_info:
                        self.start_response(response.status,
                                            response.get_headers(), exc_info)
                    #
                    # Do the actual writing
                    loop = self._loop
                    start = loop.time()
                    for chunk in response:
                        if isawaitable(chunk):
                            chunk = await chunk
                        result = self.write(chunk)
                        if isawaitable(result):
                            await result
                        time_in_loop = loop.time() - start
                        if time_in_loop > MAX_TIME_IN_LOOP:
                            get_logger(environ).debug(
                                'Released the event loop after %.3f seconds',
                                time_in_loop)
                            await sleep(0.1, loop=self._loop)
                            start = loop.time()
                    #
                    # make sure we write headers and last chunk if needed
                    result = self.write(b'', True)
                    if isawaitable(result):
                        await result

            # client disconnected, end this connection
            except (IOError, AbortWsgi, RuntimeError):
                self.event('post_request').fire()
            except Exception:
                if wsgi_request(environ).cache.handle_wsgi_error:
                    self.keep_alive = False
                    self._write_headers()
                    self.connection.close()
                    self.event('post_request').fire()
                else:
                    done = False
                    exc_info = sys.exc_info()
            else:
                if loop.get_debug():
                    logger = get_logger(environ)
                    log_wsgi_info(logger.info, environ, self.status)
                    if not self.keep_alive:
                        logger.debug('No keep alive, closing connection %s',
                                     self.connection)
                self.event('post_request').fire()
                if not self.keep_alive:
                    self.connection.close()
            finally:
                close_object(response)

    def is_chunked(self):
        '''Check if the response uses chunked transfer encoding.

        Only use chunked responses when the client is speaking HTTP/1.1
        or newer and there was no Content-Length header set.
        '''
        if (self._status == '200 Connection established' or
                has_empty_content(int(self.status[:3])) or
                self.version == '1.0'):
            return False
        elif self.headers.get(TRANSFER_ENCODING) == 'chunked':
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
            headers[TRANSFER_ENCODING] = 'chunked'
            headers.pop(CONTENT_LENGTH, None)
        else:
            headers.pop(TRANSFER_ENCODING, None)
        if self.keep_alive:
            self.keep_alive = keep_alive_with_status(self._status, headers)
        if not self.keep_alive:
            headers[CONNECTION] = 'close'
        return headers

    def wsgi_environ(self):
        # return a the WSGI environ dictionary
        transport = self.transport
        https = True if transport.get_extra_info('sslcontext') else False
        environ = wsgi_environ(self.body_reader,
                               self.version,
                               self.raw_uri,
                               self.method,
                               self.headers,
                               transport.get_extra_info('sockname'),
                               self.address,
                               self.headers,
                               self.SERVER_SOFTWARE,
                               https=https,
                               extra=(('pulsar.connection', self.connection),
                                      ('pulsar.cfg', self.cfg)))
        self.keep_alive = self.parser.should_keep_alive()
        self.headers[SERVER] = self.SERVER_SOFTWARE
        timestamp = time.time()
        self.headers[DATE] = http_date(timestamp)
        return environ

    def _new_request(self, _, exc=None):
        connection = self._connection
        connection.data_received(self._buffer)

    def _write_headers(self):
        if not self._headers_sent:
            if self.content_length:
                self.headers[CONTENT_LENGTH] = '0'
            self.write(b'')
