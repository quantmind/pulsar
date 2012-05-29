import socket

import pulsar
from pulsar.apps.test import unittest


class TestSockUtils(unittest.TestCase):
    
    def testClient(self):
        sock = pulsar.create_client_socket(('',8080))
        self.assertFalse(sock.is_server())
        self.assertEqual(sock.name,('0.0.0.0', 0))
        
    def test_socket_pair(self):
        connection, client = pulsar.server_client_sockets(blocking=1)
        self.assertEqual(client.write(b'ciao'), 4)
        self.assertEqual(connection.recv(), b'ciao')
        self.assertEqual(connection.write(b'ciao a te'), 9)
        self.assertEqual(client.recv(), b'ciao a te')
        client.close()
        self.assertTrue(client.closed)
        self.assertEqual(connection.write(b'bla'), 3)
        self.assertRaises(socket.error, connection.recv)
        