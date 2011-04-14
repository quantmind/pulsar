# -*- coding: utf-8 -
#
# Initial file from gunicorn.
# http://gunicorn.org/
# Adapted for Python 3 compatibility and to work with pulsar
#
# Original GUNICORN LICENCE
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import re
    
from pulsar.http.utils import to_string
from pulsar.http.globals import *
from .body import ChunkedReader, LengthReader, EOFReader, Body


class Message(object):
    hdrre = re.compile("[\x00-\x1F\x7F()<>@,;:\[\]={} \t\\\\\"]")
    
    def __init__(self, unreader):
        self.unreader = unreader
        self.version = None
        self.headers = []
        self.trailers = []
        self.body = None
        unused = self.parse(self.unreader)
        self.unreader.unread(unused)
        self.set_body_reader()
    
    def parse(self):
        raise NotImplementedError()

    def parse_headers(self, data):
        headers = []
        
        lines = [to_string(line) + "\r\n" for line in data.split(CRLF)]

        # Parse headers into key/value pairs paying attention
        # to continuation lines.
        while len(lines):
            # Parse initial header name : value pair.
            curr = lines.pop(0)
            if curr.find(":") < 0:
                raise InvalidHeader(curr.strip())
            name, value = curr.split(":", 1)
            name = name.rstrip(" \t").upper()
            if self.hdrre.search(name):
                raise InvalidHeaderName(name)
            name, value = name.strip(), [value.lstrip()]
            
            # Consume value continuation lines
            while len(lines) and lines[0].startswith((" ", "\t")):
                value.append(lines.pop(0))
            value = ''.join(value).rstrip()
            
            headers.append((name, value))
        return headers

    def set_body_reader(self):
        chunked = False
        clength = None
        for (name, value) in self.headers:
            if name == "CONTENT-LENGTH":
                try:
                    clength = int(value)
                except ValueError:
                    clength = None
            elif name == "TRANSFER-ENCODING":
                chunked = value.lower() == "chunked"
            elif name == "SEC-WEBSOCKET-KEY1":
                clength = 8

            if clength is not None or chunked:
                break

        if chunked:
            self.body = Body(ChunkedReader(self, self.unreader))
        elif clength is not None:
            self.body = Body(LengthReader(self.unreader, clength))
        else:
            self.body = Body(EOFReader(self.unreader))

    def should_close(self):
        for (h, v) in self.headers:
            if h == "CONNECTION":
                v = v.lower().strip()
                if v == "close":
                    return True
                elif v == "keep-alive":
                    return False
                break
        return self.version <= (1, 0)


class Request(Message):
    methre = re.compile("[A-Z0-9$-_.]{3,20}")
    versre = re.compile("HTTP/(\d+).(\d+)")
        
    def __init__(self, unreader):
        self.method = None
        self.uri = None
        self.scheme = None
        self.host = None
        self.port = 80
        self.path = None
        self.query = None
        self.fragment = None
        super(Request, self).__init__(unreader)

    def get_data(self, unreader, buf, stop=False):
        data = unreader.read()
        if not data:
            if stop:
                raise StopIteration()
            raise NoMoreData(buf.getvalue())
        buf.write(data)
    
    def parse(self, unreader):
        buf = BytesIO()

        self.get_data(unreader, buf, stop=True)
        
        # Request line
        idx = buf.getvalue().find(CRLF)
        while idx < 0:
            self.get_data(unreader, buf)
            idx = buf.getvalue().find(CRLF)
        self.parse_request_line(buf.getvalue()[:idx])
        rest = buf.getvalue()[idx+2:] # Skip CRLF
        buf = BytesIO()
        buf.write(rest)
       
        
        # Headers
        idx = buf.getvalue().find(CRLF2)

        done = buf.getvalue()[:2] == CRLF
        while idx < 0 and not done:
            self.get_data(unreader, buf)
            idx = buf.getvalue().find(CRLF2)
            done = buf.getvalue()[:2] == CRLF
             
        if done:
            self.unreader.unread(buf.getvalue()[2:])
            return ""

        self.headers = self.parse_headers(buf.getvalue()[:idx])

        ret = buf.getvalue()[idx+4:]
        buf = BytesIO()
        return ret
    
    def parse_request_line(self, line):
        bits = to_string(line).split(None, 2)
        if len(bits) != 3:
            raise InvalidRequestLine(line)

        # Method
        if not self.methre.match(bits[0]):
            raise InvalidRequestMethod(bits[0])
        self.method = bits[0].upper()

        # URI
        self.uri = bits[1]
        parts = urlparse(bits[1])
        self.scheme = parts.scheme or ''
        self.host = parts.netloc or None
        if parts.port is None:
            self.port = 80
        else:
            self.host = self.host.rsplit(":", 1)[0]
            self.port = parts.port
        self.path = parts.path or ""
        self.query = parts.query or ""
        self.fragment = parts.fragment or ""

        # Version
        match = self.versre.match(bits[2])
        if match is None:
            raise InvalidHTTPVersion(bits[2])
        self.version = (int(match.group(1)), int(match.group(2)))

    def set_body_reader(self):
        super(Request, self).set_body_reader()
        if isinstance(self.body.reader, EOFReader):
            self.body = Body(LengthReader(self.unreader, 0))


