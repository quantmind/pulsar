import os
import socket
import tempfile
import time

import pulsar
from pulsar import platform
from pulsar.utils.sockets import is_closed
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest, mock


class TestSocketsUtilities(unittest.TestCase):
    
    def test_tcp_server_socket(self):
        self.assertRaises(RuntimeError, pulsar.create_socket, ':')
        sock = pulsar.create_socket(':0', bindto=True, backlog=1)
        self.assertTrue(str(sock)[:8], '0.0.0.0:')
        self.assertEqual(sock.address[0], '0.0.0.0')
        self.assertEqual(sock.FAMILY, socket.AF_INET)
        self.assertFalse(is_closed(sock))
        _sock = sock._sock
        sock.close()
        self.assertTrue(is_closed(_sock))
        
    def test_tcp_client_server_socket(self):
        listener = pulsar.create_socket(':0', bindto=True, backlog=0)
        client = pulsar.create_socket(listener.address)
        client.setblocking(3)
        client.connect(listener.address)
        self.assertTrue(client.address)
        # Get the server socket
        server, addr = listener.accept()
        self.assertEqual(addr, client.address)
        client2 = pulsar.create_socket(listener.address)
        client2.setblocking(3)
        client2.connect(listener.address)
        server2, addr = listener.accept()
        self.assertEqual(addr, client2.address)
        
    def __test_get_socket_timeout(self):
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
        
    def __test_socket_pair(self):
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
        
    def __test_invalid_address(self):
        self.assertRaises(RuntimeError, pulsar.create_socket,
                          ('jsdchbjscbhd.com', 9000))
        
    def __testRepr(self):
        sock = pulsar.create_client_socket(':0')
        self.assertTrue(repr(sock))
        self.assertTrue(sock.info().startswith('client '))
        fd = sock.fileno()
        state = sock.__getstate__()
        self.assertEqual(fd, state['fd'])
        
    def __test_parse_address(self):
        a = pulsar.parse_address('bla.com')
        self.assertEqual(a, ('bla.com', 8000))
        self.assertRaises(RuntimeError, pulsar.parse_address, 'bla.com:ciao')
        
    @unittest.skipUnless(platform.has_multiProcessSocket, 'Posix platform only')
    def __testSerialise(self):
        sock = pulsar.create_socket('127.0.0.1:0')
        v = pickle.dumps(sock)
        sock2 = pickle.loads(v)
        self.assertEqual(sock.name, sock2.name)
        sock.close()
        self.assertEqual(sock.name, None)
        self.assertTrue(sock.closed)
        self.assertEqual(sock.write(b'bla'), 0)
        
    def __testWrite(self):
        server = pulsar.create_socket('127.0.0.1:0')
        self.assertEqual(server.accept(), (None, None))
        client = pulsar.create_connection(server.name, blocking=3)
        client_connection, address = server.accept()
        count = 0
        while address is None and count < 10:
            count += 1
            yield pulsar.NOT_DONE
            client_connection, address = server.accept()
        self.assertEqual(client.address, address)
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
        
    def __testWriteError(self):
        server = pulsar.create_socket('127.0.0.1:0')
        client = pulsar.create_connection(server.name, blocking=3)
        client_connection, address = server.accept()
        count = 0
        while address is None and count < 10:
            count += 1
            yield pulsar.NOT_DONE
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
        
    def __testUnixSocket(self):
        sock = pulsar.create_socket('unix:%s' % self.tmpfile)
        self.assertEqual(sock.FAMILY, socket.AF_UNIX)
        self.assertEqual(sock.address, self.tmpfile)
        self.assertEqual(str(sock), 'unix:%s' % self.tmpfile)
        sock.close()
        self.assertFalse(os.path.exists(self.tmpfile))
        
        
