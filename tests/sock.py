import pulsar
from pulsar.utils.test import test


class TestSockUtils(test.TestCase):
    
    def testClient(self):
        sock = pulsar.create_client_socket((None,8080))
        self.assertFalse(sock.is_server())
        self.assertEqual(sock.name,('0.0.0.0', 0))
        