# -*- coding: utf-8 -
#
# This file is part of http-parser released under the MIT license. 
# See the NOTICE for more information.
import os
import sys
import io

from pulsar import lib, SERVER_SOFTWARE, Deferred
from pulsar.utils.py2py3 import is_string
from pulsar.utils.http import unquote

from .reader import HttpBodyReader


__all__ = ['HttpRequest']


class NoMoreData(Exception):
    """ exception raised when trying to parse headers but 
    we didn't get all data needed. 
    """
    
def on_headers(f):
    
    def _(self):
        if self.parser.is_headers_complete():
            return f(self)
    
    return _


class HttpRequest(Deferred):
    '''A HTTP parser providing higher-level access to a readable,
sequential io.RawIOBase object. You can use implementions of
http_parser.reader (IterReader, StringReader, SocketReader) or 
create your own.'''

    def __init__(self, stream, addr, kind=None, parsercls = None):
        """ constructor of HttpStream. 

        :attr stream: an io.RawIOBase object
        :attr kind: Int,  could be 0 to parseonly requests, 
        1 to parse only responses or 2 if we want to let
        the parser detect the type.
        """
        super(HttpRequest,self).__init__()
        kind = kind if kind is not None else lib.HTTP_BOTH
        parsercls = parsercls or lib.HttpParser
        self.client_address = addr
        self.parser = parsercls(kind=kind)
        self.stream = stream
        
    def set_actor(self, actor):
        self.stream.set_actor(actor)

    def handle(self):
        while not self.parser.is_headers_complete():
            result = self.stream.read(callback = self._execute)
            
    def _execute(self, data):
        return self.parser.execute(data,len(data))

    def url(self):
        """ get full url of the request """
        self._check_headers_complete()
        return self.parser.get_url()

    def path(self):
        """ get path of the request (url without query string and
        fragment """
        self._check_headers_complete()
        return self.parser.get_path()

    def query_string(self):
        """ get query string of the url """
        self._check_headers_complete()
        return self.parser.get_query_string()
       
    def fragment(self):
        """ get fragment of the url """
        self._check_headers_complete()
        return self.parser.get_fragment()

    def version(self):
        self._check_headers_complete()
        return self.parser.get_version()
     
    def status_code(self):
        """ get status code of a response as integer """
        self._check_headers_complete()
        return self.parser.get_status_code()

    def method(self):
        """ get HTTP method as string"""
        self._check_headers_complete()
        return self.parser.get_method()
        
    @property
    @on_headers
    def headers(self):
        """ get request/response headers """ 
        return self.parser.get_headers()

    @property
    def should_keep_alive(self):
        """ return True if the connection should be kept alive
        """
        headers = self.headers
        if headers:
            hconn = headers.get('connection')
            if hconn == "close":
                return False
            elif hconn == "keep-alive":
                return True
            return self._version == (1, 1)
        
    def body_file(self, buffering=None, binary=True, encoding=None,
            errors=None, newline=None):
        """ return the body as a buffered stream object. If binary is
        true an io.BufferedReader will be returned, else an
        io.TextIOWrapper.
        """
        self._check_headers_complete()

        if buffering is None:
            buffering = -1
        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE

        raw = HttpBodyReader(self)
        buffer = io.BufferedReader(raw, buffering)
        if binary:
            return buffer
        text = io.TextIOWrapper(buffer, encoding, errors, newline)
        return text

    def body_string(self, encoding=None, errors=None, newline=None):
        """ return body as string """
        return self.body_file(binary=False, encoding=encoding,
                newline=newline).read()
   
    def __iter__(self):
        return self

    def next(self):
        if self.parser.is_message_complete():
            raise StopIteration 

        recved = self.stream.read(callback = self.parser.execute)
        
        b = bytearray(io.DEFAULT_BUFFER_SIZE)
        recved = self.stream.readinto(b)
        if recved is None:
            recved = 0
        del b[recved:]
        nparsed = self.parser.execute(bytes(b), recved)
        assert nparsed == recved

        return bytes(b)
    
    @on_headers
    def wsgi_environ(self):
        """Get WSGI environ based on the current request """        
        parser = self.parser
        version = parser.get_version()
        
        environ = {
            "wsgi.input": self.stream,
            "wsgi.errors": sys.stderr,
            "wsgi.version": version,
            "wsgi.run_once": False,
            "SERVER_SOFTWARE": SERVER_SOFTWARE,
            "REQUEST_METHOD": parser.get_method(),
            "QUERY_STRING": parser.get_query_string(),
            "RAW_URI": parser.get_url(),
            "SERVER_PROTOCOL": parser.get_protocol(),
            "CONTENT_TYPE": "",
            "CONTENT_LENGTH": ""
        }
        
        # REMOTE_HOST and REMOTE_ADDR may not qualify the remote addr:
        # http://www.ietf.org/rfc/rfc3875
        url_scheme = "http"
        client = self.client_address or "127.0.0.1"
        forward = client
        url_scheme = "http"
        script_name = os.environ.get("SCRIPT_NAME", "")

        for header, value in parser.get_headers():
            if header == "expect":
                # handle expect
                if value == "100-continue":
                    sock.send("HTTP/1.1 100 Continue\r\n\r\n")
            elif header == 'x-forwarded-for':
                forward = value
            elif header == "x-forwarded-protocol" and value == "ssl":
                url_scheme = "https"
            elif header == "x-forwarded-ssl" and value == "on":
                url_scheme = "https"
            elif header == "host":
                server = value
            elif header == "script_name":
                script_name = value
            elif header == "content-type":
                environ['CONTENT_TYPE'] = value
                continue
            elif header == "content-length":
                environ['CONTENT_LENGTH'] = value
                continue
        
            key = 'HTTP_' + header.upper().replace('-', '_')
            environ[key] = value

        environ['wsgi.url_scheme'] = url_scheme
        
        if is_string(forward):
            # we only took the last one
            # http://en.wikipedia.org/wiki/X-Forwarded-For
            if forward.find(",") >= 0:
                forward = forward.rsplit(",", 1)[1].strip()
            remote = forward.split(":")
            if len(remote) < 2:
                remote.append('80')
        else:
            remote = forward 

        environ['REMOTE_ADDR'] = remote[0]
        environ['REMOTE_PORT'] = str(remote[1])

        if is_string(server):
            server =  server.split(":")
            if len(server) == 1:
                if url_scheme == "http":
                    server.append("80")
                elif url_scheme == "https":
                    server.append("443")
                else:
                    server.append('')
        environ['SERVER_NAME'] = server[0]
        environ['SERVER_PORT'] = server[1]
    
        path_info = parser.get_path()
        if script_name:
            path_info = path_info.split(script_name, 1)[1]
        environ['PATH_INFO'] = unquote(path_info)
        environ['SCRIPT_NAME'] = script_name
    
        return environ

