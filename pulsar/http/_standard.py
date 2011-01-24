from pulsar.utils.py2py3 import *

if ispy3k():
    from http.server import BaseHTTPRequestHandler
else:
    from BaseHTTPServer import BaseHTTPRequestHandler
    
    
class RequestParser(BaseHTTPRequestHandler):
    
    def __init__(self, request, client_address):
        BaseHTTPRequestHandler.__init__(self, request, client_address, None)
    
    def next(self):
        return self
    
    @property
    def method(self):
        return self.command
            
    
def wsgi():
    pass
