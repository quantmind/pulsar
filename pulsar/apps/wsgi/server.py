import sys
import time
import os
import logging
import socket
from wsgiref.handlers import format_date_time
from io import BytesIO

import pulsar
from pulsar import lib, make_async, is_async, AsyncSocketServer,\
                        Deferred, AsyncConnection, AsyncResponse, DeferredSend,\
                        HttpException, MAX_BODY
from pulsar.utils.httpurl import Headers, is_string, unquote,\
                                    has_empty_content, to_bytes,\
                                    host_and_port_default, mapping_iterator
from pulsar.utils import event

from .wsgi import WsgiResponse


event.create('http-headers')


__all__ = ['HttpServer', 'WsgiActorLink']


def wsgi_environ(connection, parser):
    """return a :ref:`WSGI <apps-wsgi>` compatible environ dictionary
based on the current request. If the reqi=uest headers are not ready it returns
nothing."""
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
        'CONTENT_TYPE': '',
        "CONTENT_LENGTH": '',
        'SERVER_NAME': connection.server_name,
        'SERVER_PORT': connection.server_port,
        "wsgi.multithread": False,
        "wsgi.multiprocess":False
    }
    # REMOTE_HOST and REMOTE_ADDR may not qualify the remote addr:
    # http://www.ietf.org/rfc/rfc3875
    url_scheme = "http"
    forward = connection.address
    server = None
    url_scheme = "http"
    script_name = os.environ.get("SCRIPT_NAME", "")
    headers = mapping_iterator(parser.get_headers())
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
        server =  host_and_port_default(url_scheme, server)
        environ['SERVER_NAME'] = server[0]
        environ['SERVER_PORT'] = server[1]
    path_info = parser.get_path()
    if path_info is not None:
        if script_name:
            path_info = path_info.split(script_name, 1)[1]
        environ['PATH_INFO'] = unquote(path_info)
    environ['SCRIPT_NAME'] = script_name
    return environ

def chunk_encoding(chunk, final=False):
    head = ("%X\r\n" % len(chunk)).encode('utf-8')
    return head + chunk + b'\r\n'


class HttpResponse(AsyncResponse):
    '''Handle an HTTP response for a :class:`HttpConnection`.'''
    _status = None
    _headers_sent = None
    headers = None
    MAX_CHUNK = 65536

    def default_headers(self):
        return Headers([('Server', pulsar.SERVER_SOFTWARE),
                        ('Date', format_date_time(time.time()))])

    @property
    def environ(self):
        return self.parsed_data

    @property
    def status(self):
        return self._status

    @property
    def upgrade(self):
        if self.headers:
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
The application may call start_response more than once, if and only
if the exc_info argument is provided.
More precisely, it is a fatal error to call start_response without the exc_info
argument if start_response has already been called within the current
invocation of the application.

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
            finally:
                # Avoid circular reference
                exc_info = None
        elif self._status:
            # Headers already sent. Raise error
            raise pulsar.HttpException("Response headers already sent!")
        self._status = status
        if type(response_headers) is not list:
            raise TypeError("Headers must be a list of name/value tuples")
        self.headers = self.default_headers()
        self.headers.update(response_headers)
        return self.write

    def write(self, data):
        '''The write function required by WSGI specification.'''
        head = self.send_headers(force=data)
        if head:
            self.connection.write(head)
        if data:
            self.connection.write(data)

    def __iter__(self):
        MAX_CHUNK = self.MAX_CHUNK
        conn = self.connection
        self.environ['pulsar.connection'] = self.connection
        try:
            buffer = b''
            for b in conn.wsgi_handler(self.environ, self.start_response):
                head = self.send_headers(force=b)
                if head is not None:
                    yield head
                if b:
                    if self.chunked:
                        if buffer:
                            b = buffer + b
                        while len(b) >= MAX_CHUNK:
                            chunk, b = b[:MAX_CHUNK], b[MAX_CHUNK:]
                            yield chunk_encoding(chunk)
                        buffer = b
                    else:
                        yield b
                else:
                    yield b''
            keep_alive = self.keep_alive
        except Exception as e:
            keep_alive = False
            exc_info = sys.exc_info()
            if self._headers_sent:
                conn.log.critical('Headers already sent', exc_info=exc_info)
                yield b'CRITICAL SERVER ERROR. Please Contact the administrator'
            else:
                # Create the error response
                resp = WsgiResponse(
                            content_type=self.environ.get('CONTENT_TYPE'),
                            environ=self.environ)
                data = conn.handle_http_error(resp, e)
                for b in data(self.environ, self.start_response,
                              exc_info=exc_info):
                    head = self.send_headers(force=True)
                    if head is not None:
                        yield head
                    yield b
        else:
            # make sure we send the headers
            head = self.send_headers(force=True)
            if head is not None:
                yield head
            if self.chunked:
                if buffer:
                    yield chunk_encoding(buffer)
                yield chunk_encoding(b'')
        # close connection if required
        if not keep_alive:
            self.connection.close()

    def is_chunked(self):
        '''Only use chunked responses when the client is
speaking HTTP/1.1 or newer and there was no Content-Length header set.'''
        cl = self.content_length
        if self.environ['wsgi.version'] <= (1,0):
            return False
        elif has_empty_content(int(self.status[:3])):
            # Do not use chunked responses when the response
            # is guaranteed to not have a response body.
            return False
        elif cl is not None and cl <= MAX_BODY:
            return False
        return True

    def get_headers(self, force=False):
        '''Get the headers to send only if *force* is ``True`` or this
is an HTTP upgrade (websockets)'''
        if self.upgrade or force:
            if not self._status:
                # we are sending headers but the start_response was not called
                raise HttpException('Headers not set.')
            headers = self.headers
            # Set chunked header if needed
            if self.is_chunked():
                headers['Transfer-Encoding'] = 'chunked'
                headers.pop('content-length', None)
            if 'connection' not in headers:
                connection = "keep-alive" if self.keep_alive else "close"
                headers['Connection'] = connection
            return headers

    def send_headers(self, force=False):
        if not self._headers_sent:
            tosend = self.get_headers(force)
            if tosend:
                event.fire('http-headers', tosend, sender=self)
                self._headers_sent = tosend.flat(self.version, self.status)
                return self._headers_sent

    def close(self, msg=None):
        self.connection._current_response = None
        if not self.keep_alive:
            return self.connection.close(msg)


class HttpConnection(AsyncConnection):
    '''An :class:`AsyncConnection` for HTTP servers. It produces
a :class:`HttpResponse` at every client request.'''
    response_class = HttpResponse

    @property
    def wsgi_handler(self):
        return self.actor.app_handler
    
    @property
    def server_name(self):
        return self.server.server_name
    
    @property
    def server_port(self):
        return self.server.server_port

    def handle_http_error(self, response, e):
        '''Handle an error during response.

:parameter response: a :class:`WsgiResponse`.
:parameter e: Error.
'''
        return self.actor.cfg.handle_http_error(self, response, e)


class HttpParser:
    connection = None
    def __init__(self):
        self.p = lib.Http_Parser(kind=0)
        
    def decode(self, data):
        if self.p.is_message_complete():
            self.p = lib.Http_Parser(kind=0)
        if data:
            self.p.execute(data, len(data))
            if self.p.is_message_complete():
                return wsgi_environ(self.connection, self.p), bytearray()
        return None, bytearray()
    
    
class HttpServer(AsyncSocketServer):
    connection_class = HttpConnection

    def __init__(self, *args, **kwargs):
        super(HttpServer, self).__init__(*args, **kwargs)
        host, port = self.sock.getsockname()[:2]
        self.server_name = socket.getfqdn(host)
        self.server_port = port
        
    def parser_class(self):
        return HttpParser()


class WsgiActorLink(object):
    '''A callable utility for sending :class:`ActorMessage`
to linked :class:`Actor` instances.

.. attribute:: actor_link_name

    The :attr:`pulsar.Actor.name` of the actor which will receive messages via
    the :class:`WsgiActorLink` from actors managing a wsgi server.
    An example on how to use an :class:`WsgiActorLink` can be found in the
    :class:`pulsar.apps.tasks.TaskQueueRpcMixin`, where the
    ``task_queue_manager`` attribute is a lint to the
    :class:`pulsar.apps.tasks.TaskQueue`.
'''
    def __init__(self, actor_link_name):
        self.actor_link_name = actor_link_name

    def proxy(self, sender):
        '''Get the :class:`ActorProxy` for the sender.'''
        proxy = sender.get_actor(self.actor_link_name)
        if not proxy:
            raise ValueError('Got a request from actor "{0}" which is\
not linked with "{1}".'.format(sender, self.actor_link_name))
        return proxy

    def create_callback(self, environ, action, *args, **kwargs):
        '''Create an :class:`ActorLinkCallback` for sending messages
with additional parameters.

:parameter sender: The :class:`Actor` sending the message.
:parameter action: The *action* in the :class:`ActorMessage`.
:parameter args: same as :attr:`ActorMessage.args`
:parameter kwargs: same as :attr:`ActorMessage.kwargs`
:rtype: an :class:`ActorLinkCallback`.
'''
        sender = environ.get('pulsar.connection').actor
        target = self.proxy(sender)
        return DeferredSend(sender, target, action, args, kwargs)

    def __call__(self, sender, action, *args, **kwargs):
        return self.create_callback(sender, action, *args, **kwargs)()
