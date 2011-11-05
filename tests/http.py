'''Tests the http parser'''
import unittest as test

import pulsar
from pulsar.utils.http import Headers
from pulsar import lib



class httpPythonParser(test.TestCase):
    _lib = lib.fallback
    
    @classmethod
    def get_parser(cls):
        return cls._lib.HttpParser()
    
    @classmethod
    def setUpClass(cls):
        cls.socket = pulsar.create_connection(('httpbin.ep.io',80),
                                              blocking = True)
    
    @classmethod
    def tearDownClass(cls):
        cls.socket.close()
    
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
        while True:
            data = s.recv(1024)
            if not data:
                break
            p.execute(data,len(data))
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
        while True:
            data = s.recv(1024)
            if not data:
                break
            p.execute(data,len(data))
        self.assertTrue(p.is_headers_complete())
        self.assertTrue(p.is_message_complete())
        headers = Headers(p.get_headers())
        self.assertTrue('date' in headers)
        self.assertTrue('server' in headers)
        self.assertTrue('connection' in headers)
        
        
@test.skipUnless(lib.hasextensions,'C extensions not available.')
class httpCParser(httpPythonParser):
    _lib = lib
    