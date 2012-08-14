'''Tests the wsgi middleware in pulsar.apps.wsgi'''
from pulsar.utils.httpurl import range, zip
from pulsar.apps import wsgi
from pulsar.apps.test import unittest


class wsgiTest(unittest.TestCase):
    
    def testResponse200(self):
        r = wsgi.WsgiResponse(200)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.status,'200 OK')
        self.assertEqual(r.content,())
        self.assertFalse(r.is_streamed)
        self.assertFalse(r.started)
        self.assertEqual(list(r), [])
        self.assertTrue(r.started)
        
    def testResponse500(self):
        r = wsgi.WsgiResponse(500, content = b'A critical error occurred')
        self.assertEqual(r.status_code, 500)
        self.assertEqual(r.status,'500 Internal Server Error')
        self.assertEqual(r.content,(b'A critical error occurred',))
        self.assertFalse(r.is_streamed)
        self.assertFalse(r.started)
        self.assertEqual(list(r), [b'A critical error occurred'])
        self.assertTrue(r.started)
        
    def testStreamed(self):
        stream = ('line {0}\n'.format(l+1) for l in range(10))
        r = wsgi.WsgiResponse(content=stream)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.status, '200 OK')
        self.assertEqual(r.content, stream)
        self.assertTrue(r.is_streamed)
        data = []
        for l, a in enumerate(r):
            data.append(a)
            self.assertTrue(r.started)
            self.assertEqual(a, ('line {0}\n'.format(l+1)).encode('utf-8'))
        self.assertEqual(len(data), 10)
        
        
class testWsgiApplication(unittest.TestCase):
    
    def testBuildWsgiApp(self):
        appserver = wsgi.WSGIServer()
        