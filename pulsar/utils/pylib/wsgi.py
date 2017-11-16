import os
import sys
from urllib.parse import unquote
from wsgiref.handlers import format_date_time as http_date
from multidict import istr, CIMultiDict


CHARSET = 'ISO-8859-1'
CRLF = b'\r\n'
HEAD = 'HEAD'
MAX_CHUNK_SIZE = 65536
TLS_SCHEMES = frozenset(('https', 'wss'))
NO_CONTENT_CODES = frozenset((204, 304))
NO_BODY_VERBS = frozenset((HEAD,))
URL_SCHEME = os.environ.get('wsgi.url_scheme', 'http')
OS_SCRIPT_NAME = os.environ.get("SCRIPT_NAME", "")
PULSAR_CACHE = 'pulsar.cache'
CONNECTION = istr('Connection')
CONTENT_LENGTH = istr('Content-Length')
CONTENT_TYPE = istr('Content-Type')
DATE = istr('Date')
HOST = istr('Host')
KEEP_ALIVE = istr('Keep-Alive')
LOCATION = istr('Location')
PROXY_AUTHENTICATE = istr('Proxy-Authenticate')
PROXY_AUTHORIZATION = istr('Proxy-Authorization')
SCRIPT_NAME = istr("Script_Name")
SERVER = istr('Server')
SET_COOKIE = istr('Set-Cookie')
TE = istr('Te')
TRAILERS = istr('Trailers')
TRANSFER_ENCODING = istr('Transfer-Encoding')
UPGRADE = istr('Upgrade')
X_FORWARDED_FOR = istr('X-Forwarded-For')
EXPECT = istr('Expect')
HOP_HEADERS = frozenset((
    CONNECTION, KEEP_ALIVE, PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION, TE, TRAILERS,
    TRANSFER_ENCODING, UPGRADE
))


class Headers:
    __slots__ = ()

    def X_FORWARDED_PROTOCOL(self, environ, value):
        if value == "ssl":
            environ['wsgi.url_scheme'] = 'https'

    def X_FORWARDED_PROTO(self, environ, value):
        if value in TLS_SCHEMES:
            environ['wsgi.url_scheme'] = 'https'

    def X_FORWARDED_SSL(self, environ, value):
        if value == "on":
            environ['wsgi.url_scheme'] = 'https'

    def SCRIPT_NAME(self, environ, value):
        environ['SCRIPT_NAME'] = value

    def CONTENT_TYPE(self, environ, value):
        environ['CONTENT_TYPE'] = value
        return True

    def CONTENT_LENGTH(self, environ, value):
        environ['CONTENT_LENGTH'] = value
        return True


class WsgiProtocol:
    """"Pure python WSGI protocol implementation
    """
    keep_alive = False
    parsed_url = None
    headers_sent = None
    status = None

    def __init__(self, protocol, cfg, FileWrapper):
        connection = protocol.connection
        server_address = connection.transport.get_extra_info('sockname')
        self.environ = {
            'wsgi.async': True,
            'wsgi.timestamp': protocol.producer.current_time,
            'wsgi.errors': sys.stderr,
            'wsgi.version': (1, 0),
            'wsgi.run_once': False,
            'wsgi.multithread': True,
            'wsgi.multiprocess': True,
            'SCRIPT_NAME': OS_SCRIPT_NAME,
            'SERVER_SOFTWARE': protocol.producer.server_software,
            'wsgi.file_wrapper': FileWrapper,
            'CONTENT_TYPE': '',
            'SERVER_NAME': server_address[0],
            'SERVER_PORT': str(server_address[1]),
            PULSAR_CACHE: protocol
        }
        self.body_reader = protocol.body_reader(self.environ)
        self.environ['wsgi.input'] = self.body_reader
        self.cfg = cfg
        self.headers = CIMultiDict()
        self.protocol = protocol
        self.connection = connection
        self.client_address = connection.address
        self.parser = protocol.create_parser(self)
        self.header_wsgi = Headers()

    def on_url(self, url):
        proto = self.protocol
        transport = self.connection.transport
        parsed_url = proto.parse_url(url)
        query = parsed_url.query or b''
        scheme = ('https' if transport.get_extra_info('sslcontext')
                  else URL_SCHEME)
        if parsed_url.schema:
            scheme = parsed_url.schema.decode(CHARSET)

        self.parsed_url = parsed_url
        self.environ.update((
            ('RAW_URI', url.decode(CHARSET)),
            ('RAW_URI', url.decode(CHARSET)),
            ('REQUEST_METHOD', self.parser.get_method().decode(CHARSET)),
            ('QUERY_STRING', query.decode(CHARSET)),
            ('wsgi.url_scheme', scheme)
        ))

    def on_header(self, name, value):
        header = istr(name.decode(CHARSET))
        header_value = value.decode(CHARSET)
        header_env = header.upper().replace('-', '_')

        if 'SERVER_PROTOCOL' not in self.environ:
            self.environ['SERVER_PROTOCOL'] = (
                "HTTP/%s" % self.parser.get_http_version())

        if header in HOP_HEADERS:
            if header == CONNECTION:
                if (self.environ['SERVER_PROTOCOL'] == 'HTTP/1.0'
                        or header_value.lower() != 'keep-alive'):
                    self.headers[header] = header_value
            else:
                self.headers[header] = header_value
        else:
            hnd = getattr(self.header_wsgi, header_env, None)
            if hnd and hnd(self.environ, header_value):
                return

        self.environ['HTTP_%s' % header_env] = header_value

    def on_headers_complete(self):
        if 'SERVER_PROTOCOL' not in self.environ:
            self.environ['SERVER_PROTOCOL'] = (
                "HTTP/%s" % self.parser.get_http_version()
            )

        forward = self.headers.get(X_FORWARDED_FOR)
        client_address = self.client_address

        if self.environ.get('wsgi.url_scheme') == 'https':
            self.environ['HTTPS'] = 'on'
        if forward:
            # we only took the last one
            # http://en.wikipedia.org/wiki/X-Forwarded-For
            if forward.find(",") >= 0:
                forward = forward.rsplit(",", 1)[1].strip()
            client_address = forward.split(":")
            if len(client_address) < 2:
                client_address.append('80')
        self.environ['REMOTE_ADDR'] = client_address[0]
        self.environ['REMOTE_PORT'] = str(client_address[1])

        if self.parsed_url.path is not None:
            path_info = self.parsed_url.path.decode(CHARSET)
            script_name = self.environ['SCRIPT_NAME']
            if script_name:
                path_info = path_info.split(script_name, 1)[1]
            self.environ['PATH_INFO'] = unquote(path_info)

        # add the protocol to the pipeline
        self.connection.pipeline(self.protocol)

    def on_body(self, body):
        self.body_reader.feed_data(body)

    def on_message_complete(self):
        self.body_reader.feed_eof()
        self.protocol.finished_reading()

    def start_response(self, status, response_headers, exc_info=None):
        if exc_info:
            try:
                if self.headers_sent:
                    # if exc_info is provided, and the HTTP headers have
                    # already been sent, start_response must raise an error,
                    # and should re-raise using the exc_info tuple
                    reraise(exc_info[0], exc_info[1], exc_info[2])
            finally:
                # Avoid circular reference
                exc_info = None
        elif self.status:
            # Headers already set. Raise error
            raise RuntimeError("Response headers already set!")
        else:
            self.keep_alive = self.parser.should_keep_alive()
        self.status = status
        for header, value in response_headers:
            if header in HOP_HEADERS:
                # These features are the exclusive province of this class,
                # this should be considered a fatal error for an application
                # to attempt sending them, but we don't raise an error,
                # just log a warning
                self.protocol.producer.logger.warning(
                    'Application passing hop header "%s"', header
                )
                continue
            self.headers.add(header, value)
        producer = self.protocol.producer
        self.headers[SERVER] = producer.server_software
        self.headers[DATE] = fast_http_date(producer.current_time)
        return self.write

    def write(self, data, force=False):
        buffer = None
        env = self.environ
        proto = self.protocol

        if not self.headers_sent:
            self.headers_sent = self.get_headers()
            buffer = bytearray(('%s %s\r\n' % (env['SERVER_PROTOCOL'],
                                               self.status)).encode(CHARSET))
            for k, v in self.headers_sent.items():
                buffer.extend(('%s: %s\r\n' % (k, v)).encode(CHARSET))
            buffer.extend(CRLF)
            proto.event('on_headers').fire(data=buffer)

        if data:
            if not buffer:
                buffer = bytearray()
            if self.chunked:
                http_chunks(buffer, data)
            else:
                buffer.extend(data)

        elif force and self.chunked:
            if not buffer:
                buffer = bytearray()
            http_chunks(buffer, data, True)

        if buffer:
            return self.connection.write(buffer)

    def get_headers(self):
        headers = self.headers
        chunked = headers.get(TRANSFER_ENCODING) == 'chunked'
        content_length = CONTENT_LENGTH in headers
        status = int(self.status.split()[0])
        empty = has_empty_content(status, self.environ['REQUEST_METHOD'])

        if status >= 400:
            self.keep_alive = False

        if not self.status:
            # we are sending headers but the start_response was not called
            raise RuntimeError('Headers not set.')

        if (content_length or empty or
                self.environ['SERVER_PROTOCOL'] == 'HTTP/1.0'):
            chunked = False
            headers.pop(TRANSFER_ENCODING, None)
        elif not chunked and not content_length:
            chunked = True
            headers[TRANSFER_ENCODING] = 'chunked'

        if not self.keep_alive:
            headers[CONNECTION] = 'close'

        self.chunked = chunked
        return headers


def chunk_encoding(chunks, chunk):
    '''Write a chunk::
        chunk-size(hex) CRLF
        chunk-data CRLF
    If the size is 0, this is the last chunk, and an extra CRLF is appended.
    '''
    chunks.extend(("%X\r\n" % len(chunk)).encode('utf-8'))
    chunks.extend(chunk)
    chunks.extend(CRLF)


def http_chunks(chunks, data, finish=False):
    while len(data) >= MAX_CHUNK_SIZE:
        chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
        chunk_encoding(chunks, chunk)
    if data:
        chunk_encoding(chunks, data)
    if finish:
        chunk_encoding(chunks, data)


_http_time_ = None
_http_date_ = None


def fast_http_date(timestamp):
    global _http_time_, _http_date_
    if _http_time_ != timestamp:
        _http_time_ = timestamp
        _http_date_ = http_date(timestamp)
    return _http_date_


def has_empty_content(status, method=None):
    """204, 304 and 1xx codes have no content, same for HEAD requests"""
    return (status in NO_CONTENT_CODES or
            100 <= status < 200 or
            method in NO_BODY_VERBS)


def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value
