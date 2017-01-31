import re
import sys
import zlib
from enum import Enum
from urllib.parse import urlparse

from ..exceptions import ProtocolError


METHOD_RE = re.compile(b"[A-Z0-9$-_.]{3,20}")
VERSION_RE = re.compile(b"HTTP/(\d+).(\d+)")
STATUS_RE = re.compile(b"(\d{3})\s*(\w*)")
HEADER_RE = re.compile(b"[\x00-\x1F\x7F()<>@,;:\[\]={} \t\\\\\"]")

# errors
BAD_FIRST_LINE = 0
INVALID_HEADER = 1
INVALID_CHUNK = 2

CHARSET = 'ISO-8859-1'


class InvalidRequestLine(ProtocolError):
    """error raised when first line is invalid """


class InvalidHeader(ProtocolError):
    """error raised on invalid header """


class InvalidChunkSize(ProtocolError):
    """error raised when we parse an invalid chunk size """


class HttpParserError(Exception):
    pass


class HttpParserCallbackError(HttpParserError):
    pass


class HttpParserInvalidStatusError(HttpParserError):
    pass


class HttpParserInvalidMethodError(HttpParserError):
    pass


class HttpParserInvalidURLError(HttpParserError):
    pass


class HttpParserUpgrade(Exception):
    pass


class ParserType(Enum):
    HTTP_REQUEST = 0
    HTTP_RESPONSE = 1
    HTTP_BOTH = 2


def passthrough(*args):
    pass


def parse_url(url):
    return urlparse(url)


class HttpParser:
    """A python HTTP parser.

    Original code from https://github.com/benoitc/http-parser

    2011 (c) Benoit Chesneau <benoitc@e-engura.org>
    """
    def __init__(self, protocol, kind=ParserType.HTTP_BOTH):
        # errors vars
        self.errno = None
        self.errstr = ""
        # protected variables
        self._buf = []
        self._version = None
        self._method = None
        self._status_code = None
        self._kind = kind
        self._chunked = False
        self._trailers = None
        self._partial_body = False
        self._clen = None
        self._clen_rest = None
        # private variables
        self.__on_firstline = False
        self.__on_headers_complete = False
        # protocol callbacks
        self._on_url = getattr(
            protocol, 'on_url', passthrough
        )
        self._on_header = getattr(protocol, 'on_header', passthrough)
        self._on_headers_complete = getattr(
            protocol, 'on_headers_complete', passthrough
        )
        self._on_message_begin = getattr(
            protocol, 'on_message_begin', passthrough
        )
        self._on_message_complete = getattr(
            protocol, 'on_message_complete', passthrough
        )
        self._on_body = getattr(
            protocol, 'on_body', passthrough
        )
        self.__on_message_complete = False

    @property
    def kind(self):
        return self._kind

    def get_http_version(self):
        return self._version

    def recv_body(self):
        """ return last chunk of the parsed body"""
        body = b''.join(self._body)
        self._body = []
        self._partial_body = False
        return body

    def is_headers_complete(self):
        """ return True if all headers have been parsed. """
        return self.__on_headers_complete

    def is_partial_body(self):
        """ return True if a chunk of body have been parsed """
        return self._partial_body

    def is_message_complete(self):
        """ return True if the parsing is done (we get EOF) """
        return self.__on_message_complete

    def is_chunked(self):
        """ return True if Transfer-Encoding header value is chunked"""
        return self._chunked

    def feed_data(self, data):
        # end of body can be passed manually by putting a length of 0
        if not data:
            if not self.__on_message_complete:
                self.__on_message_complete = True
                self._on_message_complete()
            return
        #
        # start to parse
        nb_parsed = 0
        while True:
            if not self.__on_firstline:
                idx = data.find(b'\r\n')
                if idx < 0:
                    self._buf.append(data)
                    return len(data)
                else:
                    self.__on_firstline = True
                    self._buf.append(data[:idx])
                    first_line = b''.join(self._buf)
                    rest = data[idx+2:]
                    data = b''
                    if self._parse_firstline(first_line):
                        nb_parsed = nb_parsed + idx + 2
                        self._buf = [rest]
                    else:
                        return nb_parsed
            elif not self.__on_headers_complete:
                if data:
                    self._buf.append(data)
                    data = b''
                try:
                    to_parse = b''.join(self._buf)
                    ret = self._parse_headers(to_parse)
                    if ret is False:
                        return
                    nb_parsed = nb_parsed + (len(to_parse) - ret)
                except InvalidHeader as e:
                    self.errno = INVALID_HEADER
                    self.errstr = str(e)
                    return nb_parsed
            elif not self.__on_message_complete:
                if data:
                    self._buf.append(data)
                    data = b''
                ret = self._parse_body()
                if ret is None:
                    return
                elif ret < 0:
                    return ret
                elif ret == 0:
                    self.__on_message_complete = True
                    self._on_message_complete()
                    return
                else:
                    nb_parsed = ret
            else:
                return 0

    def _parse_firstline(self, line):
        try:
            if self.kind == 2:  # auto detect
                try:
                    self._parse_request_line(line)
                except InvalidRequestLine:
                    self._parse_response_line(line)
            elif self.kind == ParserType.HTTP_RESPONSE:
                self._parse_response_line(line)
            else:
                self._parse_request_line(line)
        except InvalidRequestLine as e:
            self.errno = BAD_FIRST_LINE
            self.errstr = str(e)
            return False
        return True

    def _parse_response_line(self, line):
        bits = line.split(None, 1)
        if len(bits) != 2:
            raise InvalidRequestLine(line)

        # version
        matchv = VERSION_RE.match(bits[0])
        if matchv is None:
            raise InvalidRequestLine("Invalid HTTP version: %s" % bits[0])
        self._version = '%s.%s' % (int(matchv.group(1)), int(matchv.group(2)))

        # status
        matchs = STATUS_RE.match(bits[1])
        if matchs is None:
            raise InvalidRequestLine("Invalid status %" % bits[1])

        self._status = bits[1]
        self._status_code = int(matchs.group(1))
        self._reason = matchs.group(2)

    def _parse_request_line(self, line):
        bits = line.split(None, 2)
        if len(bits) != 3:
            raise InvalidRequestLine(line)
        # Method
        if not METHOD_RE.match(bits[0]):
            raise InvalidRequestLine("invalid Method: %s" % bits[0])
        self._method = bits[0].upper()
        # URI
        self._on_url(bits[1])
        # Version
        match = VERSION_RE.match(bits[2])
        if match is None:
            raise InvalidRequestLine("Invalid HTTP version: %s" % bits[2])
        self._version = '%s.%s' % (int(match.group(1)), int(match.group(2)))

    def _parse_headers(self, data):
        while True:
            idx = data.find(b'\r\n')
            if idx < 0:  # we don't have a header
                self._buf = [data]
                return False
            elif not idx:
                break
            chunk = data[:idx]
            data = data[idx+2:]
            if chunk.find(b':') < 0:
                continue
            name, value = chunk.split(b':', 1)
            value = value.lstrip()
            name = name.rstrip(b" \t").strip()
            self._on_header(name, value)
            name = name.lower()
            if name == b'content-length':
                try:
                    self._clen = int(value)
                except ValueError:
                    continue
                if self._clen < 0:  # ignore nonsensical negative lengths
                    self._clen = None
            elif name == b'transfer-encoding':
                self._chunked = value.lower() == b'chunked'
        #
        if self._clen is None:
            self._clen_rest = sys.maxsize
        else:
            self._clen_rest = self._clen

        rest = data[idx+2:]
        self._buf = [rest]
        self.__on_headers_complete = True
        self._on_headers_complete()
        return len(rest)

    def _parse_body(self):
        data = b''.join(self._buf)
        #
        if not self._chunked:
            #
            if not data and self._clen is None:
                if not self._status:    # message complete only for servers
                    self.__on_message_complete = True
                    self._on_message_complete()
            else:
                if self._clen_rest is not None:
                    self._clen_rest -= len(data)
                if data:
                    self._on_body(data)
                self._buf = []
                if self._clen_rest <= 0:
                    self.__on_message_complete = True
                    self._on_message_complete()
            return
        else:
            try:
                size, rest = self._parse_chunk_size(data)
            except InvalidChunkSize as e:
                self.errno = INVALID_CHUNK
                self.errstr = "invalid chunk size [%s]" % str(e)
                return -1
            if size == 0:
                return size
            if size is None or len(rest) < size + 2:
                return None
            body_part, rest = rest[:size], rest[size:]

            # maybe decompress
            body_part = self._decompress(body_part)
            self._partial_body = True
            self._body.append(body_part)
            rest = rest[2:]
            self._buf = [rest] if rest else []
            return len(rest) + 2

    def _parse_chunk_size(self, data):
        idx = data.find(b'\r\n')
        if idx < 0:
            return None, None
        line, rest_chunk = data[:idx], data[idx+2:]
        chunk_size = line.split(b';', 1)[0].strip()
        try:
            chunk_size = int(chunk_size, 16)
        except ValueError:
            raise InvalidChunkSize(chunk_size)
        if chunk_size == 0:
            self._parse_trailers(rest_chunk)
            return 0, None
        return chunk_size, rest_chunk

    def _parse_trailers(self, data):
        idx = data.find(b'\r\n\r\n')
        if data[:2] == b'\r\n':
            self._trailers = self._parse_headers(data[:idx])

    def _decompress(self, data):
        deco = self.__decompress_obj
        if deco is not None:
            if not self.__decompress_first_try:
                data = deco.decompress(data)
            else:
                try:
                    data = deco.decompress(data)
                except zlib.error:
                    self.__decompress_obj = zlib.decompressobj(-zlib.MAX_WBITS)
                    deco = self.__decompress_obj
                    data = deco.decompress(data)
                self.__decompress_first_try = False
        return data


class HttpRequestParser(HttpParser):

    def __init__(self, protocol):
        super().__init__(protocol, ParserType.HTTP_REQUEST)
        self._proto_on_url = getattr(protocol, 'on_url', None)

    def get_method(self):
        return self._method


class HttpResponseParser(HttpParser):

    def __init__(self, protocol):
        super().__init__(protocol, ParserType.HTTP_RESPONSE)
        self._proto_on_status = getattr(protocol, 'on_status', None)

    def get_status_code(self):
        return self._status_code

