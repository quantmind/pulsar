'''Tests the wsgi middleware in pulsar.apps.wsgi'''
from pulsar.apps import wsgi
from pulsar.utils.test import test


class wsgiTest(test.TestCase):
    
    def testResponse(self):
        r = wsgi.WsgiResponse(200)
        self.assertEqual(r.status_code,200)
        self.assertEqual(r.status,'200 OK')
        self.assertEqual(r.content,())
        self.assertFalse(r.is_streamed)