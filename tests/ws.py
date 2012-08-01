'''Tests the websocket middleware in pulsar.apps.ws.'''
from pulsar.apps import ws
from pulsar.apps.test import unittest


class WebSocketTest(unittest.TestCase):
    
    def testHyBiKey(self):
        w = ws.WebSocket(None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v,"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
        