import sys
from urllib.parse import unquote

from multidict import istr, CIMultiDict
from httptools import HttpParserInvalidURLError

from wsgi cimport _http_date


class Headers:

    def X_FORWARDED_PROTOCOL(self, dict environ, str value):
        if value == "ssl":
            environ['wsgi.url_scheme'] = 'https'

    def X_FORWARDED_PROTO(self, dict environ, str value):
        if value in TLS_SCHEMES:
            environ['wsgi.url_scheme'] = 'https'

    def X_FORWARDED_SSL(self, dict environ, str value):
        if value == "on":
            environ['wsgi.url_scheme'] = 'https'

    def SCRIPT_NAME(self, dict environ, str value):
        environ['SCRIPT_NAME'] = value

    def CONTENT_TYPE(self, dict environ, str value):
        environ['CONTENT_TYPE'] = value
        return True

    def CONTENT_LENGTH(self, dict environ, str value):
        environ['CONTENT_LENGTH'] = value
        return True


cdef class WsgiProtocol:

    cdef readonly:
        dict environ
        object headers, headers_sent, chunked, parser, connection, client_address, cfg, body_reader
    cdef public:
        str status
        object keep_alive
    cdef object header_wsgi, protocol, parsed_url

    def __cinit__(self, object protocol, object cfg, object FileWrapper):
        cdef object connection = protocol.connection
        cdef object server_address = connection.transport.get_extra_info('sockname')
        self.environ = {
            'wsgi.async': True,
            'wsgi.timestamp': connection.producer.time.current_time,
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

    cpdef on_url(self, bytes url):
        cdef object proto = self.protocol
        cdef object transport = self.connection.transport
        cdef object scheme = 'https' if transport.get_extra_info('sslcontext') else URL_SCHEME
        cdef object parsed_url

        try:
            parsed_url = proto.parse_url(url)
        except HttpParserInvalidURLError:
            parsed_url = proto.parse_url(scheme.encode(CHARSET) + b'://' + url)
        else:
            if parsed_url.schema:
                scheme = parsed_url.schema.decode(CHARSET)

        self.parsed_url = parsed_url
        self.environ.update((
            ('RAW_URI', url.decode(CHARSET)),
            ('REQUEST_METHOD', self.parser.get_method().decode(CHARSET)),
            ('QUERY_STRING', parsed_url.query.decode(CHARSET) if parsed_url.query else ''),
            ('wsgi.url_scheme', scheme)
        ))

    cpdef on_header(self, bytes name, bytes value):
        cdef object header = istr(name.decode(CHARSET))
        cdef str header_value = value.decode(CHARSET)
        cdef str header_env = header.upper().replace('-', '_')
        cdef object hnd

        if 'SERVER_PROTOCOL' not in self.environ:
            self.environ['SERVER_PROTOCOL'] = "HTTP/%s" % self.parser.get_http_version()

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

    cpdef on_headers_complete(self):
        cdef str forward = self.headers.get(X_FORWARDED_FOR)
        cdef object client_address = self.client_address
        cdef str path_info
        cdef str script_name

        if 'SERVER_PROTOCOL' not in self.environ:
            self.environ['SERVER_PROTOCOL'] = "HTTP/%s" % self.parser.get_http_version()

        if self.environ['wsgi.url_scheme'] == 'https':
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

        self.connection.pipeline(self.protocol)

    cpdef on_body(self, bytes body):
        self.body_reader.feed_data(body)

    cpdef on_message_complete(self):
        self.body_reader.feed_eof()
        self.protocol.finished_reading()

    cpdef start_response(self, str status, object response_headers, object exc_info=None):
        cdef str value
        cdef object header

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
        self.status = status
        for header, value in response_headers:
            if header in HOP_HEADERS:
                # These features are the exclusive province of this class,
                # this should be considered a fatal error for an application
                # to attempt sending them, but we don't raise an error,
                # just log a warning
                LOGGER.warning('Application passing hop header "%s"', header)
                continue
            self.headers.add(header, value)
        self.headers[SERVER] = self.environ['SERVER_SOFTWARE']
        self.headers[DATE] = fast_http_date(self.environ['wsgi.timestamp'])
        self.keep_alive = self.parser.should_keep_alive()
        return self.write

    cpdef object write(self, bytes data, object force=False):
        cdef bytearray buffer = None
        cdef dict env = self.environ
        cdef object proto = self.protocol
        cdef object tosend
        cdef str http

        if not self.headers_sent:
            http = env.get('SERVER_PROTOCOL', DEFAULT_HTTP)
            self.headers_sent = self.get_headers()
            buffer = bytearray(('%s %s\r\n' % (http, self.status)).encode(CHARSET))
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

    cdef get_headers(self):
        cdef object headers = self.headers
        cdef object chunked = headers.get(TRANSFER_ENCODING) == 'chunked'
        cdef object content_length = CONTENT_LENGTH in headers
        cdef int status = int(self.status.split()[0])

        if status >= 400:
            self.keep_alive = False

        if not self.status:
            # we are sending headers but the start_response was not called
            raise RuntimeError('Headers not set.')

        if (content_length or
                has_empty_content(status, self.environ['REQUEST_METHOD']) or
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


cdef chunk_encoding(bytearray chunks, bytes chunk):
    '''Write a chunk::

        chunk-size(hex) CRLF
        chunk-data CRLF

    If the size is 0, this is the last chunk, and an extra CRLF is appended.
    '''
    chunks.extend(("%X\r\n" % len(chunk)).encode('utf-8'))
    chunks.extend(chunk)
    chunks.extend(CRLF)


cdef http_chunks(bytearray chunks, bytes data, object finish=False):
    cdef bytes chunk
    while len(data) >= MAX_CHUNK_SIZE:
        chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
        chunk_encoding(chunks, chunk)
    if data:
        chunk_encoding(chunks, data)
    if finish:
        chunk_encoding(chunks, data)


cpdef fast_http_date(int timestamp):
    global _http_time_, _http_date_
    if _http_time_ != timestamp:
        _http_time_ = timestamp
        _http_date_ = _http_date(timestamp)
    return _http_date_


cpdef http_date(int timestamp):
    return _http_date(timestamp)


cpdef has_empty_content(int status, str method=None):
    """204, 304 and 1xx codes have no content, same for HEAD requests"""
    return (status in NO_CONTENT_CODES or
            100 <= status < 200 or
            method in NO_BODY_VERBS)


cdef reraise(object tp, object value, object tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value
