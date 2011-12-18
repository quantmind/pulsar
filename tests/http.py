'''Tests the http parser'''
import io

import pulsar
from pulsar.utils.http import Headers
from pulsar import lib
from pulsar.utils.test import test

BIN_HOST = 'httpbin.org'

def hostport(host):
    hp = host.split(':')
    if len(hp) == 2:
        return hp[0],int(hp[1])
    else:
        return host,80
    

class httpPythonParser(test.TestCase):
    host = (BIN_HOST,80)
    bufsize = io.DEFAULT_BUFFER_SIZE
    _lib = lib.fallback
    
    @classmethod
    def get_parser(cls):
        return cls._lib.HttpParser()
    
    @classmethod
    def setUpClass(cls):
        if cls.worker.cfg.http_proxy:
            host = hostport(cls.worker.cfg.http_proxy)
        else:
            host = cls.host
        cls.socket = pulsar.create_connection(host, blocking = True)
    
    @classmethod
    def tearDownClass(cls):
        cls.socket.close()
        
    def read(self, p):
        while not p.is_message_complete():
            data = self.socket.recv(self.bufsize)
            p.execute(data,len(data))
    
    def testParser(self):
        p = self.get_parser()
        self.assertFalse(p.is_headers_complete())
        self.assertFalse(p.is_message_complete())
        self.assertEqual(self._lib.HTTP_REQUEST,0)
        self.assertEqual(self._lib.HTTP_RESPONSE,1)
        self.assertEqual(self._lib.HTTP_BOTH,2)
        
    def testGet(self):
        p = self.get_parser()
        s = self.socket
        s.send(b'GET / HTTP/1.1\r\nHost: httpbin.ep.io\r\n\r\n')
        self.read(p)
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_complete())
        headers = Headers(p.get_headers())
        self.assertTrue('date' in headers)
        self.assertTrue('server' in headers)
        self.assertTrue('connection' in headers)
    
    def __testPost(self):
        p = self.get_parser()
        s = self.socket
        s.send(b'POST /post HTTP/1.1\r\nHost: httpbin.ep.io\r\n\r\n')
        self.read(p)
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_complete())
        headers = Headers(p.get_headers())
        self.assertTrue('date' in headers)
        self.assertTrue('server' in headers)
        self.assertTrue('connection' in headers)
        
        
@test.skipUnless(lib.hasextensions,'C extensions not available.')
class httpCParser(httpPythonParser):
    _lib = lib
    