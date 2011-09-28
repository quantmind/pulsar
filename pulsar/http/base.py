import pulsar
from pulsar.utils.py2py3 import *
from .wsgi import create_wsgi, Response


__all__ = ['Request']


class Request(object):
    RESPONSE_CLASS = Response
    MessageClass = None
    
    def __init__(self, stream, client_address):
        self.stream = stream
        self.client_address = client_address
        self.reader = http.HttpStream(self.stream)
        self.setup()
        self.handle()
        self.finish()
    
    def setup(self):
        pass

    def finish(self):
        pass
        
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
    
