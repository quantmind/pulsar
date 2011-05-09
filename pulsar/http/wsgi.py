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
import os
import re
import sys

from pulsar import SERVER_SOFTWARE, PickableMixin
from pulsar.utils.tools import cached_property

from .utils import is_hoppish, http_date, write, write_chunk, to_string,\
                   parse_authorization_header
from .globals import *


NORMALIZE_SPACE = re.compile(r'(?:\r\n)?[ \t]+')
EMPTY_DICT = {}
EMPTY_TUPLE = ()


class PulsarWsgiHandler(PickableMixin):
    
    def send(self, request, name, args = None, kwargs = None,
             server = None, ack = True):
        worker = request.environ['pulsar.worker']
        if server:
            server = worker.ACTOR_LINKS[server]
        else:
            server = worker.arbiter
        if name in server.remotes:
            ack = server.remotes[name]
        args = args or EMPTY_TUPLE
        kwargs = kwargs or EMPTY_DICT
        return server.send(worker.aid, (args,kwargs), name = name, ack = ack)


def create_wsgi(req, sock, client, server, cfg, worker = None):
    resp = Response(req, sock)

    environ = {
        "wsgi.input": req.body,
        "wsgi.errors": sys.stderr,
        "wsgi.version": (1, 0),
        "wsgi.multithread":  (cfg.concurrency == 'thread' and cfg.workers >1),
        "wsgi.multiprocess": (cfg.concurrency == 'process' and cfg.workers >1),
        "wsgi.run_once": False,
        "pulsar.socket": sock,
        "pulsar.worker": worker,
        "SERVER_SOFTWARE": SERVER_SOFTWARE,
        "REQUEST_METHOD": req.method,
        "QUERY_STRING": req.query,
        "RAW_URI": req.uri,
        "SERVER_PROTOCOL": "HTTP/%s" % ".".join(map(str, req.version)),
        "CONTENT_TYPE": "",
        "CONTENT_LENGTH": ""
    }
    
    # authors should be aware that REMOTE_HOST and REMOTE_ADDR
    # may not qualify the remote addr:
    # http://www.ietf.org/rfc/rfc3875
    client = client or "127.0.0.1"
    forward = client
    url_scheme = "http"
    script_name = os.environ.get("SCRIPT_NAME", "")

    for hdr_name, hdr_value in req.headers:
        if hdr_name == "EXPECT":
            # handle expect
            if hdr_value.lower() == "100-continue":
                sock.send("HTTP/1.1 100 Continue\r\n\r\n")
        elif hdr_name == "X-FORWARDED-FOR":
            forward = hdr_value
        elif hdr_name == "X-FORWARDED-PROTOCOL" and hdr_value.lower() == "ssl":
            url_scheme = "https"
        elif hdr_name == "X-FORWARDED-SSL" and hdr_value.lower() == "on":
            url_scheme = "https"
        elif hdr_name == "HOST":
            server = hdr_value
        elif hdr_name == "SCRIPT_NAME":
            script_name = hdr_value
        elif hdr_name == "CONTENT-TYPE":
            environ['CONTENT_TYPE'] = hdr_value
            continue
        elif hdr_name == "CONTENT-LENGTH":
            environ['CONTENT_LENGTH'] = hdr_value
            continue
        
        key = 'HTTP_' + hdr_name.replace('-', '_')
        environ[key] = hdr_value

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

    path_info = req.path
    if script_name:
        path_info = path_info.split(script_name, 1)[1]
    environ['PATH_INFO'] = unquote(path_info)
    environ['SCRIPT_NAME'] = script_name

    return resp, environ


class Middleware(object):
    '''Middleware handler'''
    
    def __init__(self):
        self.handles = []
        
    def add(self, handle):
        self.handles.append(handle)
        
    def apply(self, elem):
        for handle in self.handles:
            try:
                handle(elem)
            except Exception as e:
                pass


class Request(object):
    
    def __init__(self, environ):
        self.environ = environ
    
    @cached_property
    def data(self):
        return self.environ['wsgi.input'].read()
    
    @cached_property
    def authorization(self):
        """The `Authorization` object in parsed form."""
        code = 'HTTP_AUTHORIZATION'
        if code in self.environ:
            header = self.environ[code]
            return parse_authorization_header(header)
    
    
class Response(object):

    def __init__(self, req, sock):
        self.req = req
        self.sock = sock
        self.version = SERVER_SOFTWARE
        self.status = None
        self.chunked = False
        self.should_close = req.should_close()
        self.headers = []
        self.headers_sent = False

    @property
    def client_sock(self):
        return self.sock
    
    def force_close(self):
        self.should_close = True

    def start_response(self, status, headers, exc_info=None):
        if exc_info:
            try:
                if self.status and self.headers_sent:
                    raise (exc_info[0], exc_info[1], exc_info[2])
            finally:
                exc_info = None
        elif self.status is not None:
            raise AssertionError("Response headers already set!")

        self.status = status
        self.process_headers(headers)
        return self.write

    def process_headers(self, headers):
        for name, value in headers:
            name = to_string(name)
            value = to_string(value)
            if is_hoppish(name):
                lname = name.lower().strip()
                if lname == "transfer-encoding":
                    if value.lower().strip() == "chunked":
                        self.chunked = True
                elif lname == "connection":
                    # handle websocket
                    if value.lower().strip() != "upgrade":
                        continue
                else:
                    # ignore hopbyhop headers
                    continue
            self.headers.append((name.strip(), value.strip()))

    def default_headers(self):
        connection = "keep-alive"
        if self.should_close:
            connection = "close"

        return [
            "HTTP/1.1 %s\r\n" % self.status,
            "Server: %s\r\n" % self.version,
            "Date: %s\r\n" % http_date(),
            "Connection: %s\r\n" % connection
        ]

    def send_headers(self):
        if self.headers_sent:
            return
        tosend = self.default_headers()
        tosend.extend(["%s: %s\r\n" % (n, v) for n, v in self.headers])
        write(self.sock, "%s\r\n" % "".join(tosend))
        self.headers_sent = True

    def write(self, arg):
        self.send_headers()
        write(self.sock, arg, self.chunked)

    def close(self):
        if not self.headers_sent:
            self.send_headers()
        if self.chunked:
            write_chunk(self.sock, "")
