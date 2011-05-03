from pulsar.http.base import Request as RequestBase

from .parser import RequestParser


class Request(RequestBase):
    
    def setup(self):
        pass
    
    def finish(self):
        pass
    
    def parse(self):
        p = RequestParser(self.client_sock)
        return p.next()
    
