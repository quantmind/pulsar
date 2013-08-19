import socket

from pulsar import Protocol
from pulsar.utils.pep import get_event_loop, new_event_loop
from pulsar.utils.internet import is_closed
from pulsar.apps.test import unittest


class TestEventLoop(unittest.TestCase):
    
    def test_create_connection_error(self):
        loop = get_event_loop()
        try:
            result = yield loop.create_connection(Protocol,'127.0.0.1', 9898)
        except socket.error:
            pass
        
    def test_start_serving(self):
        loop = get_event_loop()
        sockets = yield loop.start_serving(Protocol,'127.0.0.1', 0)
        self.assertEqual(len(sockets), 1)
        socket = sockets[0]
        fn = socket.fileno()
        handler = loop._handlers[fn]
        self.assertTrue(handler)
        self.assertTrue(handler.handle_read)
        self.assertFalse(handler.handle_write)
        loop.stop_serving(socket)
        handler = loop._handlers.get(fn)
        self.assertFalse(handler)
        self.assertTrue(is_closed(socket))
        
    def test_start_serving_ipv6(self):
        loop = get_event_loop()
        sockets = yield loop.start_serving(Protocol,'::1', 0)
        self.assertEqual(len(sockets), 1)
        sock = sockets[0]
        self.assertEqual(sock.family, socket.AF_INET6)
        loop.stop_serving(sock)
        self.assertTrue(is_closed(sock))