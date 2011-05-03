import pulsar
from pulsar.utils.py2py3 import *
from .wsgi import create_wsgi

if ispy3k:
    from http.server import BaseHTTPRequestHandler
else:
    from BaseHTTPServer import BaseHTTPRequestHandler
    

class Request(object):
    MessageClass = None
    
    def __init__(self, client_sock, client_address, server, cfg = None):
        self.client_sock = client_sock
        self.client_address = client_address
        self.server = server
        self.cfg = cfg
        self.setup()
        self.handle()
        self.finish()
    
    def setup(self):
        r = self.client_sock
        self.rfile = r.makefile('rb', -1)
        self.wfile = r.makefile('wb', 0)

    def finish(self):
        if not self.wfile.closed:
            self.wfile.flush()
        self.wfile.close()
        self.rfile.close()
        
    def parse(self):
        self.raw_requestline = self.rfile.readline()
        if not self.parse_request(): # An error code has been sent, just exit
            return
        
    def handle(self):
        """Handle a single HTTP request"""
        req = self._req = self.parse()
        self.method = req.method
        self.path = req.path 
        
    def wsgi(self, worker = None):
        return create_wsgi(self._req,
                           self.client_sock,
                           self.client_address,
                           self.server,
                           self.cfg,
                           worker = worker)
    
