from pulsar import net
from pulsar.apps import test


class WebSocketTest(test.TestCase):
    
    def testHyBiKey(self):
        w = net.WebSocket(None)
        v = w.challenge_response('dGhlIHNhbXBsZSBub25jZQ==')
        self.assertEqual(v,"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
        
        