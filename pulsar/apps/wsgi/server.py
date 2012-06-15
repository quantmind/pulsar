import sys
import time
import os
from wsgiref.handlers import format_date_time
from io import BytesIO

import pulsar
from pulsar import lib, make_async, is_async, AsyncSocketServer,\
                        Deferred, AsyncConnection, AsyncResponse
from pulsar.utils.httpurl import Headers, iteritems, is_string, unquote,\
                                    has_empty_content, to_bytes
from pulsar.utils import event


event.create('http-headers')

__all__ = ['HttpServer', 'wsgi_iterator']


def wsgi_iterator(result, callback, *args, **kwargs):
    result = make_async(result).get_result_or_self()
    while is_async(result):
        # yield empty bytes so that the loop is released
        yield b''
        result = result.get_result_or_self()
    # The result is ready
    for chunk in callback(result, *args, **kwargs):
        yield chunk

def wsgi_environ(self):
    """return a :ref:`WSGI <apps-wsgi>` compatible environ dictionary
based on the current request. If the reqi=uest headers are not ready it returns
nothing.

In addition to all standard WSGI entries it
adds the following 2 pulsar information:

* ``pulsar.stream`` the :attr:`stream` attribute.
* ``pulsar.actor`` the :class:`pulsar.Actor` serving the request.
"""
    parser = self.parser
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
        "wsgi.multiprocess":False
    }        
    # REMOTE_HOST and REMOTE_ADDR may not qualify the remote addr:
    # http://www.ietf.org/rfc/rfc3875
    url_scheme = "http"
    forward = self.address
    server = None
    url_scheme = "http"
    script_name = os.environ.get("SCRIPT_NAME", "")

    headers = parser.get_headers()
    if isinstance(headers, dict):
        headers = iteritems(headers)
    for header, value in headers:
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


class HttpResponse(AsyncResponse):
    '''Handle one HTTP response'''
    _status = None
    _headers_sent = None
    
    def response_iterator(self):
        environ = self.environ
        self.headers = self.connection.default_headers()
        worker = self.connection.actor
        start_response = self.start_response
        try:
            return worker.app_handler(environ, start_response)
        except Exception as e:
            # we make sure headers where not sent
            try:
                start_response('500 Internal Server Error', [], sys.exc_info())
            except:
                # The headers were sent already
                self.log.critical('Headers already sent!',
                                  exc_info=sys.exc_info())
                return iter([b'Critical Server Error'])
            else:
                # Create the error response
                data = WsgiResponse(environ=environ)
                worker.cfg.handle_http_error(data, e)
                data(environ, start_response)
                return data
        
    @property
    def environ(self):
        return self.parsed_data
    
    @property
    def status(self):
        return self._status
    
    @property
    def upgrade(self):
        return self.headers.get('Upgrade')
    
    @property
    def chunked(self):
        return self.headers.get('Transfer-Encoding') == 'chunked'
    
    @property
    def content_length(self):
        c = self.headers.get('Content-Length')
        if c:
            return int(c)
    
    @property
    def version(self):
        return self.environ.get('wsgi.version')
    
    @property
    def keep_alive(self):
        """ return True if the connection should be kept alive"""
        conn = self.environ.get('HTTP_CONNECTION','').lower()
        if conn == "close":
            return False
        elif conn == "keep-alive":
            return True
        return self.version == (1, 1)
        
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
                if self._headers_sent:
                    # if exc_info is provided, and the HTTP headers have
                    # already been sent, start_response must raise an error,
                    # and should re-raise using the exc_info tuple
                    raise (exc_info[0], exc_info[1], exc_info[2])
                else:
                    # If exc_info is supplied, and no HTTP headers have been
                    # output yet, start_response should replace the
                    # currently-stored HTTP response headers with the
                    # newly-supplied ones. 
                    self.headers = self.connection.default_headers()
            finally:
                # Avoid circular reference
                exc_info = None
        elif self._headers_sent:
            # Headers already sent. Raise error
            raise pulsar.HttpException("Response headers already sent!")
        
        self._status = status
        self.headers.update(response_headers)
        return self.connection.write
    
    def __iter__(self):
        '''WSGI write function returned by the
:meth:`HttpResponse.start_response` function.

New WSGI applications and frameworks should not use this callable directly
if it is possible to avoid doing so.
In general, applications should produce their output via their returned
iterable, as this makes it possible for web servers to interleave other
tasks in the same Python thread, potentially providing better throughput
for the server as a whole.

:parameter data: an iterable over bytes.
'''
        MAX_CHUNK = 65536
        for b in self.response_iterator():
            head = self.send_headers(force=b)
            if head is not None:
                yield head
            if b:
                if self.chunked:
                    while b:
                        chunk, b = b[:MAX_CHUNK], b[MAX_CHUNK:]
                        head = ("%X\r\n" % len(chunk)).encode('utf-8')
                        yield head + chunk + b'\r\n'
                else:
                    yield b
            else:
                yield b''
        if self.chunked:
            yield b'0\r\n\r\n'
        # make sure we send the headers
        head = self.send_headers(force=True)
        if head is not None:
            yield head
        if not self.keep_alive:
            self.connection.close()

    def is_chunked(self):
        '''Only use chunked responses when the client is
speaking HTTP/1.1 or newer and there was no Content-Length header set.'''
        if self.environ['wsgi.version'] <= (1,0):
            return False
        elif has_empty_content(int(self.status[:3])):
            # Do not use chunked responses when the response
            # is guaranteed to not have a response body.
            return False
        elif self.content_length is not None and\
                 self.content_length <= self.socket.MAX_BODY: 
            return False
        return True
    
    def get_headers(self, force=False):
        '''Get the headers to send only if *force* is ``True`` or this
is an HTTP upgrade (websockets)'''
        headers = self.headers
        if self.upgrade:
            return headers
        elif force:
            # Set chunked header if needed
            if self.is_chunked():
                headers['Transfer-Encoding'] = 'chunked'
                self.headers.pop('content-length', None)
            connection = "keep-alive" if self.keep_alive else "close"
            headers['Connection'] = connection
            return headers
    
    def send_headers(self, force=False):
        if not self._headers_sent:
            tosend = self.get_headers(force)
            if tosend:
                event.fire('http-headers', tosend, sender=self)
                data = tosend.flat(self.version, self.status)
                # headers are strings, therefore we need
                # to convert them to bytes before sending them
                data = to_bytes(data)
                self._headers_sent = data
                return data
    
    def close(self, msg=None):
        self.connection._current_response = None
        if not self.keep_alive:
            return self.connection.close(msg)

        
class HttpConnection(AsyncConnection):
    '''Asynchronous HTTP Connection'''
    response_class = HttpResponse
            
    def default_headers(self):
        return Headers((('Server',pulsar.SERVER_SOFTWARE),
                        ('Date', format_date_time(time.time()))),
                       kind='server')
    
    def request_data(self):
        data = bytes(self.buffer)
        self.buffer = bytearray()
        self.parser.execute(data, len(data))
        if self.parser.is_message_complete():
            environ = wsgi_environ(self)
            # Rebuild the parser
            self.parser = self.server.parser_class()
            return environ
    
    
class HttpServer(AsyncSocketServer):
    connection_class = HttpConnection
    
    def parser_class(self):
        return lib.Http_Parser(kind=0)
    