'''Test Internet connections and wrapped socket methods in event loop.'''
import socket

from pulsar import Connection, Protocol, TcpServer, async_while
from pulsar.utils.pep import get_event_loop, new_event_loop
from pulsar.utils.internet import is_socket_closed, format_address
from pulsar.apps.test import unittest
from pulsar.async.pollers import READ

from examples.echo.manage import Echo, EchoServerProtocol

server_protocol = lambda: Connection(1, 0, EchoServerProtocol, None)

class TestEventLoop(unittest.TestCase):

    def test_create_connection_error(self):
        loop = get_event_loop()
        try:
            result = yield loop.create_connection(Protocol,'127.0.0.1', 9898)
        except socket.error:
            pass

    def test_start_serving(self):
        protocol_factory = lambda : Connection()
        loop = get_event_loop()
        sockets = yield loop.start_serving(server_protocol,'127.0.0.1', 0)
        self.assertEqual(len(sockets), 1)
        socket = sockets[0]
        fn = socket.fileno()
        events, read, write, error = loop.io.handlers(fn)
        self.assertEqual(events, READ)
        self.assertTrue(read)
        self.assertFalse(write)
        self.assertFalse(error)
        loop.stop_serving(socket)
        self.assertRaises(KeyError, loop.io.handlers, fn)
        self.assertTrue(is_socket_closed(socket))

    def test_start_serving_ipv6(self):
        loop = get_event_loop()
        sockets = yield loop.start_serving(Protocol,'::1', 0)
        self.assertEqual(len(sockets), 1)
        sock = sockets[0]
        self.assertEqual(sock.family, socket.AF_INET6)
        address = sock.getsockname()
        faddress = format_address(address)
        self.assertEqual(faddress, '[::1]:%s' % address[1])
        loop.stop_serving(sock)
        self.assertTrue(is_socket_closed(sock))

    def test_echo_serve(self):
        loop = get_event_loop()
        server = TcpServer(loop, '127.0.0.1', 0, EchoServerProtocol)
        yield server.start_serving()
        sock = server.sock
        fn = sock.fileno()
        self.assertFalse(is_socket_closed(sock))
        client = Echo()
        address = sock.getsockname()
        result = yield client.request(address, b'Hello!')
        self.assertEqual(result, b'Hello!')
        self.assertEqual(server.concurrent_connections, 1)
        result = yield client.request(address, b'ciao')
        self.assertEqual(result, b'ciao')
        self.assertEqual(server.concurrent_connections, 1)
        yield server.stop_serving()
        yield async_while(3, lambda: not is_socket_closed(sock))
        self.assertTrue(is_socket_closed(sock))
