'''Tests the websocket middleware in pulsar.apps.ws.'''
from pulsar.apps import ws
from pulsar.utils.test import test


class WebSocketTest(test.TestCase):
    
    def testHyBiKey(self):
        w = ws.WebSocket(None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v,"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
        