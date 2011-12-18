'''Tests the wsgi middleware in pulsar.apps.wsgi'''
from pulsar.utils.py2py3 import range, zip
from pulsar.apps import wsgi
from pulsar.utils.test import test


class wsgiTest(test.TestCase):
    
    def testResponse200(self):
        r = wsgi.WsgiResponse(200)
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.status,'200 OK')
        self.assertEqual(r.content,())
        self.assertFalse(r.is_streamed)
        self.assertTrue(r.when_ready.called)
        
    def testResponse500(self):
        r = wsgi.WsgiResponse(500, content = b'A critical error occurred')
        self.assertEqual(r.status_code,500)
        self.assertEqual(r.status,'500 Internal Server Error')
        self.assertEqual(r.content,(b'A critical error occurred',))
        self.assertFalse(r.is_streamed)
        self.assertTrue(r.when_ready.called)
        
    def testStreamed(self):
        stream = ('line {0}\n'.format(l+1) for l in range(10))
        r = wsgi.WsgiResponse(content = stream)
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.status,'200 OK')
        self.assertEqual(r.content,None)
        self.assertTrue(r.is_streamed)
        self.assertFalse(r.when_ready.called)
        data = list(r)
        self.assertEqual(len(data),10)
        for l,a in enumerate(data):
            self.assertEqual(a,('line {0}\n'.format(l+1)).encode('utf-8'))
        self.assertTrue(r.when_ready.called)
        self.assertFalse(r.is_streamed)
        