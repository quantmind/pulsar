import sys
from urllib.parse import unquote

import cython
from multidict import istr, CIMultiDict

from wsgi cimport _http_date


@cython.internal
cdef class Headers:

    cdef void x_forwarded_protocol(self, dict environ, str value):
        if value == "ssl":
            environ['wsgi.url_scheme'] = 'https'


    cdef void x_forwarded_proto(self, dict environ, str value):
        if value in TLS_SCHEMES:
            environ['wsgi.url_scheme'] = 'https'


    cdef void x_forwarded_ssl(self, dict environ, str value):
        if value == "on":
            environ['wsgi.url_scheme'] = 'https'


    cdef void script_name(self, dict environ, str value):
        environ['SCRIPT_NAME'] = value


    cdef object content_type(self, dict environ, str value):
        environ['CONTENT_TYPE'] = value
        return True


    cdef object content_length(self, dict environ, str value):
        environ['CONTENT_LENGTH'] = value
        return True



cdef class WsgiProtocol:
    cdef readonly:
        dict environ
        object headers, headers_sent, chunked, parser, connection, client_address, cfg
    cdef public:
        str status
        object keep_alive
    cdef object header_wsgi, protocol, parsed_url

    def __init__(self, object protocol, object cfg,
                 object cache, object FileWrapper):
        cdef object connection = protocol.connection
        cdef object server_address = connection.transport.get_extra_info('sockname')
        cache.connection = connection
        self.environ = {
            'wsgi.timestamp': _current_time_,
            'wsgi.errors': sys.stderr,
            'wsgi.version': (1, 0),
            'wsgi.run_once': False,
            'wsgi.multithread': True,
            'wsgi.multiprocess': True,
            'SCRIPT_NAME': OS_SCRIPT_NAME,
            'SERVER_SOFTWARE': cfg.server_software,
            'wsgi.file_wrapper': FileWrapper,
            'CONTENT_TYPE': '',
            'SERVER_NAME': server_address[0],
            'SERVER_PORT': server_address[1],
            PULSAR_CACHE: cache
        }
        self.cfg = cfg
        self.headers = CIMultiDict()
        self.protocol = protocol
        self.connection = connection
        self.client_address = connection.address
        self.parser = protocol.create_parser(self)
        self.header_wsgi = Headers()

    cpdef void on_url(self, bytes url):
        cdef object proto = self.protocol
        cdef object transport = self.connection.transport
        cdef object parsed_url = proto.parse_url(url)
        cdef bytes query = parsed_url.query or b''
        cdef object scheme = 'https' if transport.get_extra_info('sslcontext') else URL_SCHEME
        if parsed_url.schema:
            scheme = parsed_url.schema.decode(CHARSET)

        self.parsed_url = parsed_url
        self.environ.update((
            ('RAW_URI', url.decode(CHARSET)),
            ('REQUEST_METHOD', self.parser.get_method().decode(CHARSET)),
            ('QUERY_STRING', query.decode(CHARSET)),
            ('wsgi.url_scheme', scheme)
        ))

    cpdef void on_header(self, bytes name, bytes value):
        cdef object header = istr(name.decode(CHARSET))
        cdef str header_value = value.decode(CHARSET)
        cdef object hnd

        if 'SERVER_PROTOCOL' not in self.environ:
            self.environ['SERVER_PROTOCOL'] = "HTTP/%s" % self.parser.get_http_version()

        if header in HOP_HEADERS:
            if header == CONNECTION:
                if self.environ['SERVER_PROTOCOL'] == 'HTTP/1.0' or header_value == 'close':
                    self.headers[header] = header_value
            else:
                self.headers[header] = header_value
        else:
            hnd = getattr(self.header_wsgi, header, None)
            if hnd and hnd(self.environ, header_value):
                return
        self.environ['HTTP_%s' % header.upper().replace('-', '_')] = header_value

    cpdef void on_headers_complete(self):
        cdef str forward = self.headers.get(X_FORWARDED_FOR)
        cdef object client_address = self.client_address
        cdef str path_info
        cdef str script_name

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

        self.protocol._loop.create_task(self.protocol._response())

    cpdef on_body(self, bytes body):
        cdef object proto = self.protocol
        if not proto.body_reader.reader:
            proto.body_reader.initialise(
                self.request_headers, self.parser, proto.connection.transport,
                proto.cfg.stream_buffer, loop=proto._loop
            )
        proto.body_reader.feed_data(body)

    cpdef on_message_complete(self):
        self.protocol.body_reader.feed_eof()

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
        self.headers[DATE] = http_date(_current_time_)
        self.keep_alive = self.parser.should_keep_alive()
        return self.write

    cpdef object write_list(self, bytes data, object force=False):
        cdef list chunks = None
        cdef dict env = self.environ
        cdef object proto = self.protocol
        cdef object tosend

        if not self.headers_sent:
            self.headers_sent = self.get_headers()
            chunks = [('%s %s' % (env['SERVER_PROTOCOL'], self.status)).encode(CHARSET)]
            chunks.append(CRLF)
            for k, v in self.headers_sent.items():
                chunks.append(('%s: %s' % (k, v)).encode(CHARSET))
                chunks.append(CRLF)
            chunks.append(CRLF)
            proto.event('on_headers').fire(data=chunks)

        if data:
            if not chunks:
                chunks = []
            if self.chunked:
                http_chunks_l(chunks, data)
            else:
                chunks.append(data)

        elif force and self.chunked:
            if not chunks:
                chunks = []
            http_chunks_l(chunks, data, True)

        if chunks:
            return proto._connection.write(b''.join(chunks))

    cpdef object write(self, bytes data, object force=False):
        cdef bytes chunks = b''
        cdef dict env = self.environ
        cdef object proto = self.protocol
        cdef object tosend

        if not self.headers_sent:
            self.headers_sent = self.get_headers()
            chunks = ('%s %s\r\n' % (env['SERVER_PROTOCOL'], self.status)).encode(CHARSET)
            for k, v in self.headers_sent.items():
                chunks += ('%s: %s\r\n' % (k, v)).encode(CHARSET)
            chunks += b'\r\n'
            proto.event('on_headers').fire(data=chunks)

        if data:
            if self.chunked:
                chunks = http_chunks(chunks, data)
            else:
                chunks += data

        elif force and self.chunked:
            chunks = http_chunks(chunks, data, True)

        if chunks:
            return proto._connection.write(chunks)

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

        if chunked and (
                content_length or
                self.status == '200 Connection established' or
                has_empty_content(status) or
                self.environ['SERVER_PROTOCOL'] == 'HTTP/1.0'):
            chunked = False
            headers.pop(TRANSFER_ENCODING)
        elif not chunked and not content_length:
            chunked = True
            headers[TRANSFER_ENCODING] = 'chunked'

        if not self.keep_alive:
            headers[CONNECTION] = 'close'
        elif 'upgrade' in headers.get(CONNECTION, ''):
            headers[CONNECTION] = 'upgrade'

        self.chunked = chunked
        return headers


cdef void chunk_encoding_l(list chunks, bytes chunk):
    '''Write a chunk::

        chunk-size(hex) CRLF
        chunk-data CRLF

    If the size is 0, this is the last chunk, and an extra CRLF is appended.
    '''
    chunks.append(("%X\r\n" % len(chunk)).encode('utf-8'))
    chunks.append(chunk)
    chunks.append(CRLF)


cdef void http_chunks_l(list chunks, bytes data, object finish=False):
    cdef bytes chunk
    while len(data) >= MAX_CHUNK_SIZE:
        chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
        chunk_encoding_l(chunks, chunk)
    if data:
        chunk_encoding_l(chunks, data)
    if finish:
        chunk_encoding_l(chunks, data)


cdef bytes chunk_encoding(bytes total, bytes chunk):
    '''Write a chunk::

        chunk-size(hex) CRLF
        chunk-data CRLF

    If the size is 0, this is the last chunk, and an extra CRLF is appended.
    '''
    cdef bytes le = ("%X\r\n" % len(chunk)).encode('utf-8')
    return total + le + chunk


cdef bytes http_chunks(bytes chunks, bytes data, object finish=False):
    cdef bytes chunk
    while len(data) >= MAX_CHUNK_SIZE:
        chunk, data = data[:MAX_CHUNK_SIZE], data[MAX_CHUNK_SIZE:]
        chunks = chunk_encoding(chunks, chunk)
    if data:
        chunks = chunk_encoding(chunks, data)
    if finish:
        chunks = chunk_encoding(chunks, b'')
    return chunks


cpdef http_date(int timestamp):
    global _http_time_, _http_date_
    if _http_time_ != timestamp:
        _http_time_ = timestamp
        _http_date_ = _http_date(timestamp)
    return _http_date_


cpdef has_empty_content(int status, str method=None):
    """204, 304 and 1xx codes have no content, same for HEAD requests"""
    return (status in NO_CONTENT_CODES or
            100 <= status < 200 or
            method == 'HEAD')


cdef void reraise(object tp, object value, object tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value
