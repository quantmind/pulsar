import email.parser
import asyncio
import json

from http.client import HTTPMessage, _MAXHEADERS
from io import BytesIO
from urllib.parse import parse_qs
from base64 import b64encode
from functools import reduce
from cgi import valid_boundary, parse_header
from inspect import isawaitable
from asyncio import ensure_future
from typing import Dict, Callable

from multidict import MultiDict

from pulsar.api import HttpException, BadRequest
from pulsar.utils.system import convert_bytes
from pulsar.utils.structures import mapping_iterator
from pulsar.utils.httpurl import (CHARSET, ENCODE_BODY_METHODS,
                                  JSON_CONTENT_TYPES, parse_options_header)
from .headers import CONTENT_LENGTH


FORM_ENCODED_TYPES = ('application/x-www-form-urlencoded',
                      'application/x-url-encoded')
BODY_DATA = 0
BODY_FILES = 1
LARGE_BODY_CODE = 403


def http_protocol(parser):
    return "HTTP/%s" % parser.get_http_version()


class HttpBodyReader:
    """Asynchronous body reader and parser

    An instance of this class is injected into the wsgi.input key
    of the WSGI environment
    """
    __slots__ = ('limit', 'transport', 'environ',
                 '_reader', '_expect_sent', '_waiting')

    def __init__(self, transport, limit, environ):
        self.limit = limit
        self.transport = transport
        self.environ = environ
        self._reader = asyncio.StreamReader()
        self._expect_sent = None
        self._waiting = None
        self._reader.set_transport(self.transport)

    def feed_data(self, data):
        self._reader.feed_data(data)

    def feed_eof(self):
        self._reader.feed_eof()

    def fail(self):
        if self._waiting_expect():
            raise HttpException(status=417)

    def read(self, n=-1):
        self._can_continue()
        return self._reader.read(n=n)

    async def readline(self):
        self._can_continue()
        try:
            line = await self._reader.readuntil(b'\n')
        except asyncio.streams.LimitOverrunError as exc:
            line = await self.read(exc.consumed) + await self.readline()
            if len(line) > self.limit:
                raise_large_body_error(self.limit)
        return line

    def readexactly(self, n):
        self._can_continue()
        return self._reader.readexactly(n)

    def _waiting_expect(self):
        '''``True`` when the client is waiting for 100 Continue.
        '''
        if self._expect_sent is None:
            if self.environ.get('HTTP_EXPECT', '').lower() == '100-continue':
                return True
            self._expect_sent = ''
        return False

    def _can_continue(self):
        if self._waiting_expect():
            protocol = self.environ.get('SERVER_PROTOCOL')
            if protocol == 'HTTP/1.0':
                raise HttpException(status=417)
            else:
                msg = '%s 100 Continue\r\n\r\n' % protocol
                self._expect_sent = msg
                self.transport.write(msg.encode(CHARSET))


def parse_form_data(request, stream=None, **kw):
    '''Parse form data from an request and return a (forms, files) tuple.

    Both tuple values are dictionaries with the form-field name as a key
    (unicode) and lists as values (multiple values per key are possible).
    The forms-dictionary contains form-field values as unicode strings.
    The files-dictionary contains :class:`MultipartPart` instances, either
    because the form-field was a file-upload or the value is to big to fit
    into memory limits.

    :parameter request: Request object (WSGI environ wrapper).
    :parameter stream: Optional callable accepting one parameter only, the
        instance of :class:`FormDecoder` being parsed. If provided, the
        callable is invoked when data or partial data has been successfully
        parsed.
    '''
    if request.method not in ENCODE_BODY_METHODS:
        raise HttpException(status=422)

    content_type = request.get('CONTENT_TYPE')
    if content_type:
        content_type, options = parse_options_header(content_type)
        options.update(kw)
    else:
        options = kw

    if content_type == 'multipart/form-data':
        decoder = MultipartDecoder(request, options, stream)
    elif content_type in FORM_ENCODED_TYPES:
        decoder = UrlEncodedDecoder(request, options, stream)
    elif content_type in JSON_CONTENT_TYPES:
        decoder = JsonDecoder(request, options, stream)
    else:
        decoder = BytesDecoder(request, options, stream)

    return decoder.parse()


class FormDecoder:
    """Base class for decoding HTTP body data
    """
    def __init__(self, request, options: Dict, stream: Callable) -> None:
        self.request = request
        self.options = options
        self.stream = stream
        self.limit = request.cache.cfg.stream_buffer
        self.result = (MultiDict(), MultiDict())

    @property
    def content_length(self):
        return int(self.request.get('CONTENT_LENGTH', -1))

    @property
    def charset(self):
        return self.options.get('charset', 'utf-8')

    def parse(self):
        """Parse data
        """
        raise NotImplementedError


class MultipartDecoder(FormDecoder):
    buffer = None

    @property
    def boundary(self):
        return self.options.get('boundary', '')

    def parse(self):
        boundary = self.boundary
        if not valid_boundary(boundary):
            raise HttpException("Invalid boundary for multipart/form-data",
                                status=422)
        inp = self.request.get('wsgi.input') or BytesIO()
        self.buffer = bytearray()

        if isinstance(inp, HttpBodyReader):
            return self._consume(inp, boundary)
        else:
            producer = BytesProducer(inp)
            return producer(self._consume, boundary)

    async def _consume(self, fp, boundary):
        sep = b'--'
        nextpart = ('--%s' % boundary).encode()
        lastpart = ("--%s--" % boundary).encode()
        terminator = b""

        while terminator != lastpart:
            nbytes = -1
            current = None

            if terminator:
                headers = await parse_headers(fp)
                current = MultipartPart(self, headers)

                data = await fp.read(nbytes) if nbytes > 0 else b''

                if current.name:
                    current.feed_data(data)
                else:
                    current = None

            while 1:
                line = await fp.readline() or lastpart
                if line.startswith(sep):
                    terminator = line.rstrip()
                    if terminator in (nextpart, lastpart):
                        break
                if current:
                    current.feed_data(line)

            # Done with part.
            if current:
                current.done()

        self.request.environ['wsgi.input'] = BytesIO(self.buffer)
        return self.result


class BytesDecoder(FormDecoder):

    def parse(self, mem_limit=None, **kw):
        if self.content_length > self.limit:
            raise_large_body_error(self.limit)
        inp = self.request.get('wsgi.input') or BytesIO()
        data = inp.read()

        if isawaitable(data):
            return ensure_future(self._async(data))
        else:
            return self._ready(data)

    async def _async(self, chunk):
        chunk = await chunk
        return self._ready(chunk)

    def _ready(self, data):
        self.request.environ['wsgi.input'] = BytesIO(data)
        self.result = (data, None)
        return self.result


class UrlEncodedDecoder(BytesDecoder):

    def _ready(self, data):
        self.request.environ['wsgi.input'] = BytesIO(data)
        charset = self.charset
        data = parse_qs(data.decode(charset), keep_blank_values=True)
        forms = self.result[0]
        for key, values in mapping_iterator(data):
            for value in values:
                forms.add(key, value)
        return self.result


class JsonDecoder(BytesDecoder):

    def _ready(self, data):
        self.request.environ['wsgi.input'] = BytesIO(data)
        try:
            self.result = (json.loads(data.decode(self.charset)), None)
        except Exception as exc:
            raise BadRequest('Could not decode JSON') from exc
        return self.result


class MultipartPart:
    filename = None
    name = ''

    def __init__(self, parser, headers):
        self.parser = parser
        self.headers = headers
        self._bytes = []
        self._done = False
        length = headers.get(CONTENT_LENGTH)
        content = headers.get('content-disposition')
        if length:
            try:
                length = int(length)
            except ValueError:
                length = -1
        else:
            length = -1
        self.length = length
        if content:
            key, params = parse_header(content)
            name = params.get('name')
            if key == 'form-data' and name:
                self.name = name
                self.filename = params.get('filename')

    def __repr__(self):
        return self.name

    @property
    def charset(self):
        return self.parser.charset

    @property
    def content_type(self):
        return self.headers.get('Content-Type')

    @property
    def size(self):
        return reduce(lambda x, y: x+len(y), self._bytes, 0)

    def bytes(self):
        '''Bytes'''
        return b''.join(self._bytes)

    def bytesio(self):
        return BytesIO(self.recv())

    def base64(self, charset=None):
        '''Data encoded as base 64'''
        return b64encode(self.bytes()).decode(charset or self.charset)

    def string(self, charset=None):
        '''Data decoded with the specified charset'''
        return self.bytes().decode(charset or self.charset)

    def complete(self):
        return self._done

    def feed_data(self, data):
        """Feed new data into the MultiPart parser or the data stream"""
        if data:
            self._bytes.append(data)
            if self.parser.stream:
                self.parser.stream(self)
            else:
                self.parser.buffer.extend(data)

    def recv(self, size=-1):
        if self._done:
            data = self._bytes
            self._bytes = []
        else:
            data = self._bytes[:-1]
            self._bytes = self._bytes[-1:]
        return b''.join(data)

    def is_file(self):
        return self.filename or self.content_type not in (None, 'text/plain')

    def done(self):
        if not self._done:

            if self._bytes and self.length < 0:
                # Strip final line terminator
                line = self._bytes[-1]
                if line[-2:] == b"\r\n":
                    line = line[:-2]
                elif line[-1:] == b"\n":
                    line = line[:-1]
                self._bytes[-1] = line

            self._done = True
            if self.parser.stream:
                self.parser.stream(self)

            if self.is_file():
                self.parser.result[1].add(self.name, self)
            else:
                self.parser.result[0].add(self.name, self.string())


async def parse_headers(fp, _class=HTTPMessage):
    """Parses only RFC2822 headers from a file pointer.
    email Parser wants to see strings rather than bytes.
    But a TextIOWrapper around self.rfile would buffer too many bytes
    from the stream, bytes which we later need to read as bytes.
    So we read the correct bytes here, as bytes, for email Parser
    to parse.
    """
    headers = []
    while True:
        line = await fp.readline()
        headers.append(line)
        if len(headers) > _MAXHEADERS:
            raise HttpException("got more than %d headers" % _MAXHEADERS)
        if line in (b'\r\n', b'\n', b''):
            break
    hstring = b''.join(headers).decode('iso-8859-1')
    return email.parser.Parser(_class=_class).parsestr(hstring)


def raise_large_body_error(limit):
    raise HttpException(
        "Request content length too large. Limit is %s" %
        convert_bytes(limit),
        status=LARGE_BODY_CODE
    )


class BytesProducer:

    def __init__(self, bytes):
        self.bytes = bytes

    async def readline(self):
        return self.bytes.readline()

    async def read(self):
        return self.bytes.read()

    def __call__(self, consumer, *args):
        value = None
        coro = consumer(self, *args)
        while True:
            try:
                value = coro.send(value)
            except StopIteration as exc:
                value = exc.value
                break
        return value
