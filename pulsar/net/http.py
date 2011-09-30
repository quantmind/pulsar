import os
import sys

import pulsar
from pulsar import is_string, lib, Deferred, make_async, to_bytestring
from pulsar.utils.http import Headers, is_hoppish, http_date, unquote,\
                              to_string

from .tcp import TcpRequest, TcpResponse


__all__ = ['HttpRequest','HttpResponse']


def on_headers(f):
    
    def _(self):
        if self.parser.is_headers_complete():
            return f(self)
    
    return _


class HttpRequest(TcpRequest):
    '''A HTTP parser providing higher-level access to a readable,
sequential io.RawIOBase object. You can use implementions of
http_parser.reader (IterReader, StringReader, SocketReader) or 
create your own.'''
    default_parser = lib.HttpParser
    
    def on_init(self, kwargs):
        '''Set up event handler'''
        self.on_headers = Deferred()
        #Kick off the socket reading
        self._handle()
                
    def get_parser(self, kind = None, **kwargs):
        kind = kind if kind is not None else lib.HTTP_BOTH
        return self.parsercls(kind)
    
    @property
    @on_headers
    def version(self):
        return self.parser.get_version()
        
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
            "SERVER_SOFTWARE": pulsar.SERVER_SOFTWARE,
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

    #################################################################    
    # INTERNALS
    #################################################################
    
    def _handle(self, data = None):
        if data is not None:
            self.parser.execute(data,len(data))
        if not self.parser.is_headers_complete():
            self.stream.read(callback = self._handle)
        else:
            self.on_headers.callback(self.parser.get_headers())
        

class HttpResponse(TcpResponse):
    '''A specialized TcpResponse class for the HTTP protocol'''
    
    def on_init(self, kwargs):
        self.headers = Headers()
        self.on_headers = Deferred() # callback when headers are sent
        #
        # Internal flags
        self.__should_keep_alive = self.request.should_keep_alive
        self.__status = None
        self.__headers_sent = False
        
    @property
    def status(self):
        return self.__status
        
    @property
    def should_keep_alive(self):
        return self.__should_keep_alive
    
    def is_chunked(self):
        '''Only use chunked responses when the client is
speaking HTTP/1.1 or newer and there was no Content-Length header set.'''
        if self.clength is not None:
            return False
        elif self.request.version <= (1,0):
            return False
        elif self.status.startswith("304") or self.status.startswith("204"):
            # Do not use chunked responses when the response is guaranteed to
            # not have a response body.
            return False
        return True
    
    def force_close(self):
        self.__should_keep_alive = False
    
    def start_response(self, status, response_headers, exc_info=None):
        '''WSGI compliant ``start_response`` callable, see pep3333_.
        
:parameter status: an HTTP "status" string like "200 OK" or "404 Not Found".
:parameter response_headers: a list of ``(header_name, header_value)`` tuples.
    It must be a Python list. Each header_name must be a valid HTTP header
    field-name (as defined by RFC 2616_, Section 4.2), without a trailing
    colon or other punctuation.
:parameter exc_info: optional python ``sys.exc_info()`` tuple. This argument
    should be supplied by the application only if start_response is being
    called by an error handler. 
    
:rtype: The :meth:`HttpResponse.write` callable.
    
    
New WSGI applications and frameworks should not use the write() callable
if it is possible to avoid doing so. The write() callable is strictly a hack
to support imperative streaming APIs.
In general, applications should produce their output via their returned
iterable, as this makes it possible for web servers to interleave other
tasks in the same Python thread, potentially providing better throughput
for the server as a whole.

.. _pep3333: http://www.python.org/dev/peps/pep-3333/
.. _2616: http://www.faqs.org/rfcs/rfc2616.html
'''
        if exc_info:
            try:
                if self.__headers_sent:
                    # if exc_info is provided, and the HTTP headers have
                    # already been sent, start_response must raise an error,
                    # and should re-raise using the exc_info tuple
                    raise (exc_info[0], exc_info[1], exc_info[2])
                else:
                    # If exc_info is supplied, and no HTTP headers have been output
                    # yet, start_response should replace the currently-stored HTTP
                    # response headers with the newly-supplied ones. 
                    self.headers = Headers()
            finally:
                # Avoid circular reference
                exc_info = None
        elif self.__headers_sent:
            raise pulsar.BadHttpResponse(\
                        reason = "Response headers already sent!")
        
        self.__status = status
        self.process_headers(response_headers)
        return self.write
    
    def write(self, data):
        '''WSGI Compliant write function returned by the
:meth:`HttpResponse.start_response` function.'''
        return make_async(self.generate_data(data),self.stream.ioloop)
    
    def _write(self, data, callback = None):
        data = to_bytestring(data)
        return self.stream.write(data,callback)
    
    def process_headers(self, headers):
        for name, value in headers:
            name = to_string(name).lower().strip()
            value = to_string(value).lower().strip()
            if name == "content-length":
                self.clength = int(value)
            elif is_hoppish(name):
                if lname == "connection":
                    # handle websocket
                    if value != "upgrade":
                        continue
                else:
                    # ignore hopbyhop headers
                    continue
            self.headers[name] = value
            
    def default_headers(self):
        connection = "keep-alive" if self.should_keep_alive else "close"
        headers = [
            "HTTP/{0}.{1} {2}\r\n".format(self.req.version[0],\
                                       self.req.version[1], self.status),
            "Server: %s\r\n" % self.version,
            "Date: %s\r\n" % http_date(),
            "Connection: %s\r\n" % connection
        ]
        if self.is_chunked():
            headers.append("Transfer-Encoding: chunked\r\n")
        return headers
    
    def send_headers(self):
        if not self.__headers_sent:
            tosend = self.default_headers()
            tosend.extend(["%s: %s\r\n" % (n, v) for n, v in self.headers])
            data = '{0}\r\n'.format(''.join(tosend))
            self.__headers_sent = True
            self._write(data, self.on_headers.callback)
        return self.on_headers
        
    def generate_data(self, data):
        yield self.send_headers()
        write = self._write
        for elem in data:
            yield write(elem)
        
    def on_close(self):
        self.send_headers()
    