import os
import socket
import tempfile
import time

import pulsar
from pulsar import platform
from pulsar.async.defer import pickle
from pulsar.apps.test import unittest, mock
from pulsar.utils.sock import create_tcp_socket_address


class TestSockUtils(unittest.TestCase):
    
    def testClientSocket(self):
        sock = pulsar.create_client_socket(('', 8080))
        self.assertFalse(sock.is_server)
        self.assertTrue(sock.name in (None, ('0.0.0.0', 0)))
        sock2 = pulsar.create_client_socket('0.0.0.0:8080')
        self.assertFalse(sock2.is_server)
        self.assertTrue(sock2.name in (None, ('0.0.0.0', 0)))
        
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
        
    def test_parse_address(self):
        a = pulsar.parse_address('bla.com')
        self.assertEqual(a, ('bla.com', 8000))
        self.assertRaises(RuntimeError, pulsar.parse_address, 'bla.com:ciao')
        
    @unittest.skipUnless(platform.has_multiProcessSocket, 'Posix platform only')
    def testSerialise(self):
        sock = pulsar.create_socket(':0')
        v = pickle.dumps(sock)
        sock2 = pickle.loads(v)
        self.assertEqual(sock.name, sock2.name)
        sock.close()
        self.assertEqual(sock.name, None)
        self.assertTrue(sock.closed)
        self.assertEqual(sock.write(b'bla'), 0)
        
    def testWrite(self):
        server = pulsar.create_socket(':0')
        self.assertEqual(server.accept(), (None, None))
        client = pulsar.create_connection(server.name, blocking=3)
        client_connection, address = server.accept()
        self.assertEqual(client.name, address)
        client_connection = pulsar.wrap_socket(client_connection)
        self.assertEqual(client_connection.write(b''), 0)
        self.assertEqual(client_connection.write(b'ciao'), 4)
        client_connection.close()
        self.assertTrue(client_connection.closed)
        data = client.recv()
        self.assertEqual(data, b'ciao')
        data = client.recv()
        # The socket has shutdown
        self.assertEqual(data, b'')
        
    def testWriteError(self):
        server = pulsar.create_socket(':0')
        client = pulsar.create_connection(server.name, blocking=3)
        client_connection, address = server.accept()
        self.assertEqual(client.name, address)
        client_connection = pulsar.wrap_socket(client_connection)
        client_connection.send = mock.MagicMock(return_value=0)
        self.assertRaises(socket.error, client_connection.write, b'ciao')
        self.assertTrue(client_connection.closed)
        

@unittest.skipUnless(platform.is_posix, 'Posix platform only')
class UnixSocket(unittest.TestCase):
    
    def setUp(self):
        self.tmpfile = tempfile.mktemp()
        
    def tearDown(self):
        if os.path.exists(self.tmpfile):
            os.remove(self.tmpfile)
        
    def testUnixSocket(self):
        sock = pulsar.create_socket(self.tmpfile)
        self.assertEqual(sock.FAMILY, socket.AF_UNIX)
        self.assertEqual(sock.name, self.tmpfile)
        self.assertEqual(str(sock), 'unix:%s' % self.tmpfile)
        sock.close()
        self.assertFalse(os.path.exists(self.tmpfile))
        
        
