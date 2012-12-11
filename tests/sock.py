import socket
import time

import pulsar
from pulsar.apps.test import unittest, mock
from pulsar.utils.sock import create_tcp_socket_address


class TestSockUtils(unittest.TestCase):
    
    def testClientSocket(self):
        sock = pulsar.create_client_socket(('', 8080))
        self.assertFalse(sock.is_server)
        self.assertEqual(sock.name, None)
        sock2 = pulsar.create_client_socket('0.0.0.0:8080')
        self.assertFalse(sock2.is_server)
        self.assertEqual(sock2.name, None)
        
    def test_get_socket_timeout(self):
        self.assertEqual(pulsar.get_socket_timeout(None), None)
        self.assertEqual(pulsar.get_socket_timeout(-2.3), None)
        self.assertEqual(pulsar.get_socket_timeout('-1'), None)
        self.assertEqual(pulsar.get_socket_timeout(-1), None)
        self.assertEqual(pulsar.get_socket_timeout(0.0), 0)
        self.assertEqual(pulsar.get_socket_timeout(0),0)
        self.assertEqual(pulsar.get_socket_timeout(False),0)
        self.assertEqual(pulsar.get_socket_timeout(1.0),1)
        self.assertEqual(pulsar.get_socket_timeout(3.2),3.2)
        self.assertEqual(pulsar.get_socket_timeout(5),5)
        
    def test_socket_pair(self):
        server_connection, client = pulsar.server_client_sockets(blocking=1)
        self.assertEqual(client.write(b'ciao'), 4)
        self.assertEqual(server_connection.recv(), b'ciao')
        self.assertEqual(server_connection.write(b'ciao a te'), 9)
        self.assertEqual(client.recv(), b'ciao a te')
        # shut down server connection
        server_connection.close()
        self.assertTrue(server_connection.closed)
        time.sleep(0.2)
        self.assertEqual(client.write(b'ciao'), 4)
        
    def test_invalid_address(self):
        self.assertRaises(RuntimeError, pulsar.create_socket,
                          ('jsdchbjscbhd.com', 9000))
        
    def testRepr(self):
        sock = pulsar.create_client_socket(':0')
        self.assertTrue(repr(sock))
        self.assertTrue(sock.info().startswith('client '))
        fd = sock.fileno()
        state = sock.__getstate__()
        self.assertEqual(fd, state['fd'])
        
    def testForCoverage(self):
        self.assertRaises(ValueError, create_tcp_socket_address, ('',))
        self.assertRaises(ValueError, create_tcp_socket_address, ('','a'))
        self.assertRaises(TypeError, create_tcp_socket_address, ('bla'))
        