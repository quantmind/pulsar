'''Test Internet connections and wrapped socket methods in event loop.'''
import socket
import unittest
from functools import partial
from asyncio import selectors

from pulsar import (Connection, Protocol, TcpServer, async_while,
                    get_event_loop)
from pulsar.utils.pep import ispy3k
from pulsar.utils.internet import is_socket_closed, format_address
from pulsar.apps.test import run_test_server

from examples.echo.manage import Echo, EchoServerProtocol


server_protocol = partial(Connection, EchoServerProtocol)


class TestEventLoop(unittest.TestCase):

    def test_create_connection_error(self):
        loop = get_event_loop()
        try:
            result = yield loop.create_connection(Protocol, '127.0.0.1', 9898)
        except socket.error:
            pass
        else:
            assert False

    def test_create_server(self):
        protocol_factory = lambda: Connection()
        loop = get_event_loop()
        server = yield loop.create_server(Protocol, '127.0.0.1', 0)
        sockets = server.sockets
        self.assertEqual(len(sockets), 1)
        sock = sockets[0]
        fn = sock.fileno()
        events, read, write, error = loop.io.handlers(fn)
        self.assertEqual(events, selectors.EVENT_READ)
        self.assertTrue(read)
        self.assertFalse(write)
        self.assertFalse(error)
        server.close()
        self.assertRaises(KeyError, loop.io.handlers, fn)
        self.assertFalse(is_socket_closed(sock))

    def test_create_server_ipv6(self):
        loop = get_event_loop()
        server = yield loop.create_server(Protocol, '::1', 0)
        sockets = server.sockets
        self.assertEqual(len(sockets), 1)
        sock = sockets[0]
        fn = sock.fileno()
        self.assertEqual(sock.family, socket.AF_INET6)
        address = sock.getsockname()
        faddress = format_address(address)
        self.assertEqual(faddress, '[::1]:%s' % address[1])
        server.close()
        self.assertRaises(KeyError, loop.io.handlers, fn)
        self.assertFalse(is_socket_closed(sock))

    def test_create_server_error(self):
        loop = get_event_loop()
        exc = None
        try:
            yield loop.create_server(Protocol, '127.0.0.1', 0, sock=1)
        except ValueError as e:
            exc = e
        assert exc
        exc = None
        try:
            yield loop.create_server(Protocol)
        except ValueError as e:
            exc = e
        assert exc

    def test_create_connection_error(self):
        loop = get_event_loop()
        exc = None
        try:
            yield loop.create_connection(Protocol, '127.0.0.1', 0, sock=1)
        except ValueError as e:
            exc = e
        assert exc
        exc = None
        try:
            yield loop.create_connection(Protocol)
        except ValueError as e:
            exc = e
        assert exc

    def test_echo_serve(self):
        loop = get_event_loop()
        server = TcpServer(server_protocol, loop, ('127.0.0.1', 0))
        yield server.start_serving()
        self.assertEqual(len(server._server.sockets), 1)
        sock = server._server.sockets[0]
        fn = sock.fileno()
        self.assertFalse(is_socket_closed(sock))
        events, read, write, error = loop.io.handlers(fn)
        self.assertEqual(events, selectors.EVENT_READ)
        self.assertTrue(read)
        self.assertFalse(write)
        self.assertFalse(error)
        echo = Echo(server.address)
        self.assertEqual(len(server._concurrent_connections), 0)
        yield self.async.assertEqual(echo(b'Hello!'), b'Hello!')
        self.assertEqual(len(server._concurrent_connections), 1)
        yield self.async.assertEqual(echo(b'ciao'), b'ciao')
        self.assertEqual(len(server._concurrent_connections), 1)
        yield server.close()
        self.assertEqual(len(server._concurrent_connections), 0)

    def __test_create_connection_local_addr(self):
        # TODO, fix this test for all python versions
        from test.support import find_unused_port
        loop = get_event_loop()
        with run_test_server(server_protocol, loop) as server:
            yield server.start_serving()
            host = server.address[0]
            port = find_unused_port()
            tr, pr = yield loop.create_connection(Protocol,
                                                  *server.address,
                                                  local_addr=(host, port))
            yield pr.event('connection_made')
            expected = tr.get_extra_info('socket').getsockname()[1]
            self.assertEqual(pr._transport, tr)
            self.assertEqual(port, expected)
            tr.close()
