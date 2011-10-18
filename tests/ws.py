'''Tests the websocket middleware in pulsar.apps.ws.'''
import unittest as test

from pulsar.apps import ws


class WebSocketTest(test.TestCase):
    
    def testHyBiKey(self):
        w = ws.WebSocket(None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v,"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
        