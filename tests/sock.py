import pulsar
from pulsar.utils.test import test


class TestSockUtils(test.TestCase):
    
    def testClient(self):
        sock = pulsar.create_client_socket(('',8080))
        self.assertFalse(sock.is_server())
        self.assertEqual(sock.name,('0.0.0.0', 0))
        
    def test_socket_pair(self):
        server, client = pulsar.server_client_sockets(blocking=1)
        client.send(b'ciao')
        self.assertEqual(server.recv(), b'ciao')
        