'''Original code from https://github.com/benoitc/http-parser

2011 (c) Benoit Chesneau <benoitc@e-engura.org>

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

Modified and adapted for pulsar.
'''
import os
import re
import sys
import zlib

from pulsar.utils.http import *


__all__ = ['HttpParser',
           'HTTP_BOTH',
           'HTTP_RESPONSE',
           'HTTP_REQUEST']


#same as http-parser
HTTP_REQUEST = 0
HTTP_RESPONSE = 1
HTTP_BOTH = 2


METHOD_RE = re.compile("[A-Z0-9$-_.]{3,20}")
VERSION_RE = re.compile("HTTP/(\d+).(\d+)")
STATUS_RE = re.compile("(\d{3})\s*(\w*)")
HEADER_RE = re.compile("[\x00-\x1F\x7F()<>@,;:\[\]={} \t\\\\\"]")

# errors
BAD_FIRST_LINE = 0
INVALID_HEADER = 1
INVALID_CHUNK = 2


class InvalidRequestLine(Exception):
    """ error raised when first line is invalid """

class InvalidHeader(Exception):
    """ error raised on invalid header """

class InvalidChunkSize(Exception):
    """ error raised when we parse an invalid chunk size """


class HttpParser(object):
    '''A python version of the http-parser'''
    def __init__(self, kind=2, decompress=False):
        self.kind = kind
        self.decompress = decompress

        # errors vars
        self.errno = None
        self.errstr = ""
        
        # protected variables
        self._buf = [] 
        self._version = None
        self._method = None
        self._server_protocol = None
        self._status_code = None
        self._status = None
        self._reason = None
        self._url = None
        self._path = None
        self._query_string = None
        self._fragment= None
        self._headers = Headers()
        self._chunked = False
        self._body = []
        self._trailers = None
        self._partial_body = False
        self._clen = None
        self._clen_rest = None
        
        # private events
        self.__on_firstline = False
        self.__on_headers_complete = False
        self.__on_message_begin = False
        self.__on_message_complete = False
        
        self.__decompress_obj = None
        
    def get_version(self):
        return self._version

    def get_method(self):
        return self._method

    def get_status_code(self):
        return self._status_code

    def get_url(self):
        return self._url

    def get_path(self):
        return self._path

    def get_query_string(self):
        return self._query_string

    def get_fragment(self):
        return self._fragment

    def get_headers(self):
        return self._headers
    
    def get_protocol(self):
        return self._server_protocol
    
    def get_body(self):
        return self._body

    def recv_body(self):
        """ return last chunk of the parsed body"""
        body = b''.join(self._body)
        self._body = []
        self._partial_body = False
        return body

    def recv_body_into(self, barray):
        """ Receive the last chunk of the parsed bodyand store the data
        in a buffer rather than creating a new string. """
        l = len(barray)
        body = b''.join(self._body)
        m = min(len(body), l)
        data, rest = body[:m], body[m:]
        barray[0:m] = data
        if not rest:
            self._body = []
            self._partial_body = False
        else:
            self._body = [rest]
        return m

    def is_upgrade(self):
        """ Do we get upgrade header in the request. Useful for
        websockets """
        return self._headers.get('connection') == "upgrade"

    def is_headers_complete(self):
        """ return True if all headers have been parsed. """ 
        return self.__on_headers_complete

    def is_partial_body(self):
        """ return True if a chunk of body have been parsed """
        return self._partial_body

    def is_message_begin(self):
        """ return True if the parsing start """
        return self.__on_message_begin

    def is_message_complete(self):
        """ return True if the parsing is done (we get EOF) """
        return self.__on_message_complete

    def is_chunked(self):
        """ return True if Transfer-Encoding header value is chunked"""
        return self._chunked
        
    def execute(self, data, length):
        # end of body can be passed manually by putting a length of 0
        if length == 0:
            self.__on_message_complete = True
            return length

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
                    first_line = bytes_to_str(b''.join(self._buf))
                    nb_parsed = nb_parsed + idx + 2
                    
                    rest = data[idx+2:]
                    data = b''
                    if self._parse_firstline(first_line):
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
                    if not ret:
                        return length
                    nb_parsed = nb_parsed + (len(to_parse) - ret)
                except InvalidHeader as e:
                    self.errno = INVALID_HEADER 
                    self.errstr = str(e)
                    return nb_parsed
            elif not self.__on_message_complete:
                if not self.__on_message_begin:
                    self.__on_message_begin = True

                if data:
                    self._buf.append(data)
                    data = b''

                ret = self._parse_body()
                if ret is None:
                    return length

                elif ret < 0:
                    return ret
                elif ret == 0:
                    self.__on_message_complete = True
                    return length
                else:
                    nb_parsed = max(length, ret)

            else:
                return 0

    def _parse_firstline(self, line):
        try:
            if self.kind == 2: # auto detect
                try:
                    self._parse_request_line(line)
                except InvalidRequestLine:
                    self._parse_response_line(line)
            elif self.kind == 1:
                self._parse_response_line(line)
            elif self.kind == 0:
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
        self._version = (int(matchv.group(1)), int(matchv.group(2)))
            
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
        self._url = bits[1]
        parts = urlsplit(bits[1])
        self._path = parts.path or ""
        self._query_string = parts.query or ""
        self._fragment = parts.fragment or ""
        self._server_protocol = bits[2]
        
        # Version
        match = VERSION_RE.match(bits[2])
        if match is None:
            raise InvalidRequestLine("Invalid HTTP version: %s" % bits[2])
        self._version = (int(match.group(1)), int(match.group(2)))
    
    def _parse_headers(self, data):
        idx = data.find(b'\r\n\r\n')
        if idx < 0: # we don't have all headers
            return False

        # Split lines on \r\n keeping the \r\n on each line
        lines = [bytes_to_str(line) + "\r\n" for line in
                data[:idx].split(b'\r\n')]
       
        # Parse headers into key/value pairs paying attention
        # to continuation lines.
        while len(lines):
            # Parse initial header name : value pair.
            curr = lines.pop(0)
            if curr.find(":") < 0:
                raise InvalidHeader("invalid line %s" % curr.strip())
            name, value = curr.split(":", 1)
            name = name.rstrip(" \t").upper()
            if HEADER_RE.search(name):
                raise InvalidHeader("invalid header name %s" % name)
            name, value = name.strip(), [value.lstrip()]
            
            # Consume value continuation lines
            while len(lines) and lines[0].startswith((" ", "\t")):
                value.append(lines.pop(0))
            value = ''.join(value).rstrip()
            
            # multiple headers
            if name in self._headers:
                value = "%s,%s" % (self._headers[name], value)

            # store new header value
            self._headers[name] = value

        # detect now if body is sent by chunks.
        clen = self._headers.get('content-length')
        te = self._headers.get('transfer-encoding', '').lower()

        if clen is not None:
            try:
                self._clen_rest = self._clen = int(clen)
            except ValueError:
                pass
        else:
            self._chunked = (te == 'chunked')
            if not self._chunked:
                self._clen_rest = sys.maxsize

        # detect encoding and set decompress object 
        encoding = self._headers.get('content-encoding')
        if encoding == "gzip":
            self.__decompress_obj = zlib.decompressobj(16+zlib.MAX_WBITS)
        elif encoding == "deflate":
            self.__decompress_obj = zlib.decompressobj()
    
        rest = data[idx+4:]
        self._buf = [rest]
        self.__on_headers_complete = True
        return len(rest)

    def _parse_body(self):
        if not self._chunked:
            body_part = b''.join(self._buf)
            self._clen_rest -= len(body_part)

            # maybe decompress
            if self.__decompress_obj is not None:
                body_part = self.__decompress_obj.decompress(body_part)

            self._partial_body = True
            self._body.append(body_part)
            self._buf = []

            if self._clen_rest <= 0:
                self.__on_message_complete = True
            return  
        else:
            data = b''.join(self._buf)
            try:

                size, rest = self._parse_chunk_size(data)
            except InvalidChunkSize as e:
                self.errno = INVALID_CHUNK
                self.errstr = "invalid chunk size [%s]" % str(e)
                return -1

            if size == 0:
                return size

            if size is None or len(rest) < size:
                return None
            

            body_part, rest = rest[:size], rest[size:]
            if len(rest) < 2:
                self.errno = INVALID_CHUNK
                self.errstr = "chunk missing terminator [%s]" % data
                return -1

            # maybe decompress
            if self.__decompress_obj is not None:
                body_part = self.__decompress_obj.decompress(body_part)

            self._partial_body = True
            self._body.append(body_part)

            self._buf = [rest[2:]]
            return len(rest)

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
