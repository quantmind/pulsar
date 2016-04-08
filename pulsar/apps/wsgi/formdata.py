import email.parser
import asyncio
from http.client import HTTPMessage, _MAXLINE, _MAXHEADERS
from io import BytesIO
from urllib.parse import parse_qs
from base64 import b64encode
from functools import reduce
from cgi import valid_boundary, parse_header

from pulsar import HttpException, BadRequest, isawaitable, ensure_future
from pulsar.utils.system import json
from pulsar.utils.structures import MultiValueDict, mapping_iterator
from pulsar.utils.httpurl import (DEFAULT_CHARSET, ENCODE_BODY_METHODS,
                                  JSON_CONTENT_TYPES, parse_options_header)

__all__ = ['parse_form_data']


ONEMB = 2**20
# Default max size for body when not streaming
DEFAULT_MAXSIZE = 10*ONEMB

FORM_ENCODED_TYPES = ('application/x-www-form-urlencoded',
                      'application/x-url-encoded')
BODY_DATA = 0
BODY_FILES = 1
LARGE_BODY_CODE = 403


def http_protocol(parser):
    version = parser.get_version()
    return "HTTP/%s" % ".".join(('%s' % v for v in version))


class HttpBodyReader:
    _expect_sent = None
    _waiting = None

    def __init__(self, headers, parser, transport, **kw):
        self.headers = headers
        self.parser = parser
        self.reader = asyncio.StreamReader(**kw)
        self.reader.set_transport(transport)
        self.feed_data = self.reader.feed_data
        self.feed_eof = self.reader.feed_eof

    def waiting_expect(self):
        '''``True`` when the client is waiting for 100 Continue.
        '''
        if self._expect_sent is None:
            if (not self.reader.at_eof() and
                    self.headers.has('expect', '100-continue')):
                return True
            self._expect_sent = ''
        return False

    def can_continue(self):
        if self.waiting_expect():
            if self.parser.get_version() < (1, 1):
                raise HttpException(status=417)
            else:
                msg = '%s 100 Continue\r\n\r\n' % http_protocol(self.parser)
                self._expect_sent = msg
                self.reader._transport.write(msg.encode(DEFAULT_CHARSET))

    def fail(self):
        if self.waiting_expect():
            raise HttpException(status=417)

    def read(self, n=-1):
        self.can_continue()
        return self.reader.read(n=n)

    def readline(self):
        self.can_continue()
        return self.reader.readline()

    def readexactly(self, n):
        self.can_continue()
        return self.reader.readexactly(n)


def parse_form_data(environ, stream=None, **kw):
    '''Parse form data from an environ dict and return a (forms, files) tuple.

    Both tuple values are dictionaries with the form-field name as a key
    (unicode) and lists as values (multiple values per key are possible).
    The forms-dictionary contains form-field values as unicode strings.
    The files-dictionary contains :class:`MultipartPart` instances, either
    because the form-field was a file-upload or the value is to big to fit
    into memory limits.

    :parameter environ: A WSGI environment dict.
    :parameter stream: Optional callable accepting one parameter only, the
        instance of :class:`FormDecoder` being parsed. If provided, the
        callable is invoked when data or partial data has been successfully
        parsed.
    '''
    method = environ.get('REQUEST_METHOD', 'GET').upper()
    if method not in ENCODE_BODY_METHODS:
        raise HttpException(status=422)

    content_type = environ.get('CONTENT_TYPE')
    if content_type:
        content_type, options = parse_options_header(content_type)
        options.update(kw)
    else:
        options = kw

    if content_type == 'multipart/form-data':
        decoder = MultipartDecoder(environ, options, stream)
    elif content_type in FORM_ENCODED_TYPES:
        decoder = UrlEncodedDecoder(environ, options, stream)
    elif content_type in JSON_CONTENT_TYPES:
        decoder = JsonDecoder(environ, options, stream)
    else:
        decoder = BytesDecoder(environ, options, stream)

    return decoder.parse()


class FormDecoder:

    def __init__(self, environ, options, stream):
        self.environ = environ
        self.options = options
        self.stream = stream
        self.result = (MultiValueDict(), MultiValueDict())

    @property
    def content_length(self):
        return int(self.environ.get('CONTENT_LENGTH', -1))

    @property
    def charset(self):
        return self.options.get('charset', 'utf-8')

    def parse(self):
        raise NotImplementedError


class MultipartDecoder(FormDecoder):
    boundary = None

    def parse(self):
        boundary = self.options.get('boundary', '')
        if not valid_boundary(boundary):
            raise HttpException("Invalid boundary for multipart/form-data",
                                status=422)
        inp = self.environ.get('wsgi.input') or BytesIO()
        self.buffer = bytearray()

        if isinstance(inp, HttpBodyReader):
            return ensure_future(self._consume(inp, boundary),
                                 loop=inp.reader._loop)
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
            data = None
            current = None

            if terminator:
                headers = await parse_headers(fp)
                current = MultipartPart(self, headers)

                if nbytes > 0:
                    data = await fp.read(nbytes)
                else:
                    data = b''

                if current.name:
                    current.feed_data(data)
                else:
                    current = None

            while 1:
                line = await fp.readline()
                line = line or lastpart
                if line.startswith(sep):
                    terminator = line.rstrip()
                    if terminator in (nextpart, lastpart):
                        break
                if current:
                    current.feed_data(line)

            # Done with part.
            if current:
                current.done()

        self.environ['wsgi.input'] = BytesIO(self.buffer)
        return self.result


class BytesDecoder(FormDecoder):

    def parse(self, mem_limit=None, **kw):
        mem_limit = mem_limit or DEFAULT_MAXSIZE
        if self.content_length > mem_limit:
            raise HttpException("Request to big. Increase MAXMEM.",
                                status=LARGE_BODY_CODE)
        inp = self.environ.get('wsgi.input') or BytesIO()
        data = inp.read()

        if isawaitable(data):
            return ensure_future(self._async(data))
        else:
            return self._ready(data)

    async def _async(self, chunk):
        chunk = await chunk
        return self._ready(chunk)

    def _ready(self, data):
        self.environ['wsgi.input'] = BytesIO(data)
        self.result = (data, None)
        return self.result


class UrlEncodedDecoder(BytesDecoder):

    def _ready(self, data):
        self.environ['wsgi.input'] = BytesIO(data)
        charset = self.charset
        data = parse_qs(data.decode(charset), keep_blank_values=True)
        forms = self.result[0]
        for key, values in mapping_iterator(data):
            for value in values:
                forms[key] = value
        return self.result


class JsonDecoder(BytesDecoder):

    def _ready(self, data):
        self.environ['wsgi.input'] = BytesIO(data)
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
        length = headers.get('content-length')
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
                self.parser.result[1][self.name] = self
            else:
                self.parser.result[0][self.name] = self.string()


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
        if len(line) > _MAXLINE:
            raise HttpException("header line")
        headers.append(line)
        if len(headers) > _MAXHEADERS:
            raise HttpException("got more than %d headers" % _MAXHEADERS)
        if line in (b'\r\n', b'\n', b''):
            break
    hstring = b''.join(headers).decode('iso-8859-1')
    return email.parser.Parser(_class=_class).parsestr(hstring)


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
