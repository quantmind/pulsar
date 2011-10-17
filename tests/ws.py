import unittest as test

from pulsar import net


class WebSocketTest(test.TestCase):
    
    def testHyBiKey(self):
        w = net.WebSocket(None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v,"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
        