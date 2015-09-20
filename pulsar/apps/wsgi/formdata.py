import json
from io import BytesIO
from asyncio import Future
from urllib.parse import parse_qs

from pulsar import HttpException
from pulsar.utils.structures import MultiValueDict, mapping_iterator
from pulsar.utils.httpurl import DEFAULT_CHARSET, ENCODE_BODY_METHODS
from pulsar.utils.multipart import (MultipartError, MultipartParser,
                                    parse_options_header)
from pulsar.async.futures import chain_future

__all__ = ['parse_form_data']


ONEMB = 2**20
# Default max size for body when not streaming
DEFAULT_MAXSIZE = 10*ONEMB

FORM_ENCODED_TYPES = ('application/x-www-form-urlencoded',
                      'application/x-url-encoded')


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


def parse_form_data(environ, stream=None, **kw):
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
    method = environ.get('REQUEST_METHOD', 'GET').upper()
    if method not in ENCODE_BODY_METHODS:
        raise MultipartError("Request method not valid.")

    content_type = environ.get('CONTENT_TYPE')
    if not content_type:
        raise MultipartError("Missing Content-Type header.")
    content_type, options = parse_options_header(content_type)
    options.update(kw)

    if content_type == 'multipart/form-data':
        decoder = MultipartDecoder(environ, options, stream)
    elif content_type in FORM_ENCODED_TYPES:
        decoder = UrlEncoded(environ, options, stream)
    elif content_type in JSON_CONTENT_TYPES:
        decoder = JsonDecoder(environ, options, stream)
    else:
        raise MultipartError("Unsupported content type.")

    return decoder.parse()


class FormDecoder:

    def __init__(self, environ, options, stream):
        self.environ = environ
        self.options = options
        self.stream = stream

    @property
    def content_length(self):
        return int(self.environ.get('CONTENT_LENGTH', -1))

    @property
    def charset(self):
        return int(self.options.get('charset', 'utf-8'))

    def parse(self):
        raise NotImplementedError


class MultipartDecoder(FormDecoder):
    boundary = None

    def parse(self):
        self.boundary = self.options.get('boundary')
        if not self.boundary:
            raise MultipartError("No boundary for multipart/form-data.")
        self.separator = '--{0}'.format(self.boundary).encode()
        self.terminator = '--{0}--'.format(self.boundary).encode()
        inp = self.environ.get('wsgi.input') or BytesIO()

        if isinstance(inp, HttpStreamReader):
            inp.set_streaming_callback(self._recv)
            return self.read()
        else:
            data = inp.read()
            
        
        if isinstance(data, Future):
            return chain_future(data, self._recv)
        else:
            self.ready(data)

    def _recv(self, data):
        pass


class UrlEncodedDecoder(FormDecoder):

    def parse(self, mem_limit=None, **kw):
        mem_limit = mem_limit or DEFAULT_MAXSIZE
        if self.content_length > mem_limit:
            raise MultipartError("Request to big. Increase MAXMEM.")
        data = inp.read(mem_limit)

        if isinstance(data, Future):
            return chain_future(data, self.ready)
        else:
            self.ready(data)

    def ready(self, data):
        charset = self.options['charset']
        forms, files = MultiValueDict(), MultiValueDict()
        data = parse_qs(data.decode(charset), keep_blank_values=True)
        for key, values in mapping_iterator(data):
            for value in values:
                forms[key] = value
        if self.stream:
            self.stream((forms, files))
        else:
            return (forms, files)


def JsonDecoder(UrlEncodedDecoder):

    def ready(self, data):
        charset = self.options.get('charset', 'utf-8')
        forms = json.loads(data.decode(charset))
        if self.stream:
            self.stream((forms, None))
        else:
            return (forms, None)
