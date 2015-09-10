from io import BytesIO
from asyncio import Future

from pulsar import HttpException
from pulsar.utils.structures import MultiValueDict, mapping_iterator
from pulsar.utils.httpurl import DEFAULT_CHARSET, ENCODE_BODY_METHODS
from pulsar.utils.multipart import (MultipartError, MultipartParser,
                                    parse_options_header)

__all__ = ['parse_form_data']


ONEMB = 2**20
# Default max size for body when not streaming
DEFAULT_MAXSIZE = 10*ONEMB


def http_protocol(parser):
    version = parser.get_version()
    return "HTTP/%s" % ".".join(('%s' % v for v in version))


class HttpStreamReader:
    _expect_sent = None
    _waiting = None

    def __init__(self, headers, parser, transport=None):
        self.headers = headers
        self.parser = parser
        self.transport = transport
        self.buffer = b''
        self.on_message_complete = Future()
        self._streaming_callbacks = []

    def __repr__(self):
        if self.transport:
            return repr(self.transport)
        else:
            return self.__class__.__name__
    __str__ = __repr__

    def data_received(self):
        '''Called when new body data is received
        '''
        if self._streaming_callbacks:
            body = self.parser.recv_body()
            for callback in self._streaming_callbacks:
                callback(body)

    def set_streaming_callback(self, callback):
        if hasattr(callback, '__call__'):
            self._streaming_callbacks.append(callback)

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

    def read(self, maxbuf=None):
        '''Return bytes in the buffer.

        If the stream is not yet ready, return a :class:`asyncio.Future`
        which results in the bytes read unless a callback is given
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


class AsyncMultipartParser(MultipartParser):

    def __iter__(self):
        pass


def parse_form_data(environ, charset='utf-8', strict=False,
                    stream_callback=None, **kw):
    '''Parse form data from an environ dict and return a (forms, files) tuple.

    Both tuple values are dictionaries with the form-field name as a key
    (unicode) and lists as values (multiple values per key are possible).
    The forms-dictionary contains form-field values as unicode strings.
    The files-dictionary contains :class:`MultipartPart` instances, either
    because the form-field was a file-upload or the value is to big to fit
    into memory limits.

    :parameter environ: A WSGI environment dict.
    :parameter charset: The charset to use if unsure. (default: utf8)
    :parameter strict: If True, raise :exc:`MultipartError` on any parsing
        errors. These are silently ignored by default.
    :parameter stream_callback: a callback when the content is streamed
    '''
    forms, files = MultiValueDict(), MultiValueDict()
    try:
        if (environ.get('REQUEST_METHOD', 'GET').upper()
                not in ENCODE_BODY_METHODS):
            raise MultipartError("Request method not valid.")
        content_length = int(environ.get('CONTENT_LENGTH', '-1'))
        content_type = environ.get('CONTENT_TYPE', '')
        if not content_type:
            raise MultipartError("Missing Content-Type header.")
        content_type, options = parse_options_header(content_type)
        stream = environ.get('wsgi.input') or BytesIO()
        kw['charset'] = charset = options.get('charset', charset)

        async = isinstance(stream, HttpStreamReader)

        if isinstance(stream, HttpStreamReader):
            stream.set_streaming_callback(stream_callback)
            multi_parse = async_multi_parse
        else:
            assert not stream_callback


        if content_type == 'multipart/form-data':
            boundary = options.get('boundary', '')
            if not boundary:
                raise MultipartError("No boundary for multipart/form-data.")

            return multi_parse(stream, boundary, content_length, **kw)

        elif content_type in ('application/x-www-form-urlencoded',
                              'application/x-url-encoded'):
            mem_limit = kw.get('mem_limit', 2**20)
            if content_length > mem_limit:
                raise MultipartError("Request to big. Increase MAXMEM.")
            data = stream.read(mem_limit).decode(charset)
            if stream.read(1):  # These is more that does not fit mem_limit
                raise MultipartError("Request to big. Increase MAXMEM.")
            data = parse_qs(data, keep_blank_values=True)
            for key, values in mapping_iterator(data):
                for value in values:
                    forms[key] = value
        else:
            raise MultipartError("Unsupported content type.")
    except MultipartError:
        if strict:
            raise
    return forms, files


def async_multi_parse(stream, boundary, content_length, **kw):

    for part in MultipartParser(stream, boundary, content_length, **kw):
        if part.filename or not part.is_buffered():
            files[part.name] = part
        else:
            forms[part.name] = part.string()
