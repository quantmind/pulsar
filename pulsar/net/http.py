import os
import sys
import time
from wsgiref.handlers import format_date_time
from wsgiref.util import is_hop_by_hop

import pulsar
from pulsar import is_string, lib, Deferred, make_async, to_bytestring, NOT_DONE
from pulsar.utils.http import Headers, unquote, to_string
from pulsar.utils.py2py3 import BytesIO


from .tcp import TcpRequest, TcpResponse


__all__ = ['HttpRequest','HttpResponse']


def on_headers(f):
    
    def _(self, *args, **kwargs):
        
        if self.parser.is_headers_complete():
            return f(self, *args, **kwargs)
    
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__   # for sphinx
    return _


def on_body(f):
    
    def _(self, *args, **kwargs):
        
        if self.parser.is_message_complete():
            return f(self, *args, **kwargs)
    
    _.__name__ = f.__name__
    _.__doc__ = f.__doc__   # for sphinx
    return _


class HttpRequest(TcpRequest):
    '''A specialized :class:`TcpRequest` class for the HTTP protocol.'''    
    def on_init(self, kwargs):
        '''Set up event handler'''
        self.continue100 = False
        self.on_headers = Deferred(
                description = '{0} on_header'.format(self.__class__.__name__))
        self.on_body = Deferred(
                description = '{0} on_body'.format(self.__class__.__name__))
        #Kick off the socket reading
        self._handle()
        
    def default_parser(self):
        return lib.Http_Parser
                
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
        return Headers(self.parser.get_headers())

    @property
    def should_keep_alive(self):
        """ return True if the connection should be kept alive
        """
        headers = self.headers
        if headers:
            hconn = headers.get('connection','').lower()
            if hconn == "close":
                return False
            elif hconn == "keep-alive":
                return True
            return self.version == (1, 1)
   
    @on_body
    def wsgi_environ(self, actor = None, **kwargs):
        """return a :ref:`WSGI <apps-wsgi>` compatible environ dictionary
based on the current request. In addition to all standard WSGI entries it
adds the following 2 pulsar information:

* ``pulsar.stream`` the :attr:`stream` attribute.
* ``pulsar.actor`` the :class:`pulsar.Actor` serving the request.
"""
        parser = self.parser
        if not parser.is_headers_complete():
            return None
        version = parser.get_version()
        input = BytesIO()
        for b in parser.get_body():
            input.write(b)
        input.seek(0)
        protocol = parser.get_protocol()
        environ = {
            "wsgi.input": input,
            "wsgi.errors": sys.stderr,
            "wsgi.version": version,
            "wsgi.run_once": True,
            "wsgi.url_scheme": protocol,
            "SERVER_SOFTWARE": pulsar.SERVER_SOFTWARE,
            "REQUEST_METHOD": parser.get_method(),
            "QUERY_STRING": parser.get_query_string(),
            "RAW_URI": parser.get_url(),
            "SERVER_PROTOCOL": protocol,
            "CONTENT_TYPE": "",
            "CONTENT_LENGTH": "",
            "wsgi.multithread": False,
            "wsgi.multiprocess":False,
            "pulsar.stream": self.stream,
            "pulsar.actor": actor or self.stream.actor,
        }
        if kwargs:
            environ.update(kwargs)
        
        # REMOTE_HOST and REMOTE_ADDR may not qualify the remote addr:
        # http://www.ietf.org/rfc/rfc3875
        url_scheme = "http"
        client = self.client_address or "127.0.0.1"
        forward = client
        server = None
        url_scheme = "http"
        script_name = os.environ.get("SCRIPT_NAME", "")

        for header, value in parser.get_headers():
            header = header.lower()
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

        if server is not None:
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
        if path_info is not None:
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
        complete = self.parser.is_message_complete()
        if not self.parser.is_headers_complete():
            if complete:
                # There is no more data. we stop here.
                self.on_headers.callback(None)
                self.on_body.callback(None)
            else:
                self.stream.read(callback = self._handle)
        else:
            headers = self.parser.get_headers()
            if not self.on_headers.called:
                self.on_headers.callback(headers)
            if not complete:
                try:
                    cl = int(headers.get("content-length") or 0)
                except:
                    cl = 0
                if cl:
                    if headers.get("expect") == "100-continue" and\
                        not self.continue100:
                        self.continue100 = True
                        self.stream.write(b("HTTP/1.1 100 (Continue)\r\n\r\n"),
                                          callback = self._handle)
                    else:
                        self.stream.read(callback = self._handle)
                else:
                    self.parser.execute(b'',0)
                    self._handle()
            elif not self.on_body.called:
                self.on_body.callback(self.parser.get_body())
        

class HttpResponse(TcpResponse):
    '''A specialized TcpResponse class for the HTTP protocol which conforms
with Python WSGI for python 2 and 3.

 * Headers are python native strings (the ``str`` type, therefore strings in
   python 2 and unicode in python 3).
 * Content body are bytes (``str`` in python 2 and ``bytes`` in python 3).
 
Do not be confused however: even if Python 3 ``str`` is actually 
Unicode under the hood, the *content* of a native string is still 
restricted to bytes!

Therefore headers are converted to bytes before sending.

Status codes can be found here
https://github.com/joyent/node/blob/master/lib/http.js
'''
    middleware = []
    
    def on_init(self, kwargs):
        self.headers = Headers()
        self.on_headers = Deferred() # callback when headers are sent
        #
        # Internal flags
        self.__clength = None
        self.__upgrade = False
        self.__should_keep_alive = self.request.should_keep_alive
        self.__status = None
        self.__headers_sent = False
        self.__chunked = None
        
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self.status) 
    
    @property
    def status(self):
        return self.__status
        
    @property
    def should_keep_alive(self):
        return self.__should_keep_alive
    
    def is_chunked(self):
        '''Only use chunked responses when the client is
speaking HTTP/1.1 or newer and there was no Content-Length header set.'''
        if self.__chunked is None:
            if self.request.version <= (1,0):
                return False
            elif self.status.startswith("304") or self.status.startswith("204"):
                # Do not use chunked responses when the response is guaranteed to
                # not have a response body.
                return False
            elif self.__clength is not None and\
                     self.__clength <= self.stream.MAX_BODY: 
                return False
            return True
        else:
            return self.__chunked
    
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
                    # If exc_info is supplied, and no HTTP headers have been
                    # output yet, start_response should replace the
                    # currently-stored HTTP response headers with the
                    # newly-supplied ones. 
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
        '''WSGI write function returned by the
:meth:`HttpResponse.start_response` function.

New WSGI applications and frameworks should not use this callable
if it is possible to avoid doing so.
In general, applications should produce their output via their returned
iterable, as this makes it possible for web servers to interleave other
tasks in the same Python thread, potentially providing better throughput
for the server as a whole.

:parameter data: an iterable over bytes.
'''
        stream = self.stream
        ioloop = stream.ioloop
        MAX_CHUNK = 65536
        crlf = b'\r\n'
        upgrade = self.__upgrade
        wb = self._write
        timeout = self.timeout
        _write = lambda chunk : make_async(wb(chunk)).start(ioloop,
                                                     timeout = timeout)
        for b in data:
            # send headers only if there is data or it is an upgrade
            if b or upgrade:
                yield self.send_headers()
                if b:
                    if self.is_chunked():
                        while b:
                            tosend = b[:MAX_CHUNK]
                            b = b[MAX_CHUNK:]
                            head = ("%X" % len(tosend)).encode('utf-8')
                            chunk = head + crlf + tosend + crlf
                            n = len(chunk)
                            yield _write(chunk)
                        chunk = b'0' + crlf + crlf
                        yield _write(chunk)
                    else:
                        yield _write(b)
            else:
                # release the loop
                yield NOT_DONE
        # We make sure we send the headers
        yield self.send_headers()
    
    def _write(self, data, callback = None):
        return self.stream.write(data,callback)
    
    def process_headers(self, headers):
        for name, value in headers:
            name = to_string(name).strip()
            value = to_string(value).strip()
            self.headers[name] = value
            lname = name.lower()
            if lname == "content-length":
                self.__clength = int(value)
            elif lname == 'upgrade':
                self.__upgrade = self.headers['upgrade']
            
    def default_headers(self):
        headers = Headers([('Server',self.version),
                           ('Date', format_date_time(time.time()))])
        # Set chunked header if needed
        if self.is_chunked():
            self.__chunked = True
            headers['Transfer-Encoding'] = 'chunked'
            self.headers.pop('content-length',None)
        else:
            self.force_close()
        connection = "keep-alive" if self.should_keep_alive else "close"
        headers['Connection'] = connection
        return headers
    
    def send_headers(self, force=True):
        if not self.__headers_sent:
            if self.__upgrade:
                tosend = self.headers
            elif force:
                tosend = self.default_headers()
                tosend.extend(self.headers)
            else:
                # No data no upgrade, don't send headers
                return None
            data = tosend.flat(self.request.version,self.status)
            # headers are python native strings, therefore we need to convert
            # them to bytes before sending them
            data = to_bytestring(data)
            self.__headers_sent = data
            self._write(data, self.on_headers)
        return self.on_headers
        
    def close(self):
        '''Override close method so that the socket is closed only if
there is no upgrade.'''
        yield self.on_close()
        if not self.__upgrade == 'websocket':
            yield self.stream.close()
        yield self # return itself.
        
    def on_close(self):
        '''If status is available send headers.'''
        if self.__status:
            return self.send_headers()
    
