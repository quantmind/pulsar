import os
import io
import socket
import tempfile
import time

import pulsar
from pulsar import platform
from pulsar.utils.internet import (parse_address, parse_connection_string,
                                   socketpair, close_socket, is_socket_closed,
                                   format_address)
from pulsar.utils.pep import pickle
from pulsar.apps.test import unittest, mock


class TestParseAddress(unittest.TestCase):

    def test_parse_ipv4(self):
        address = parse_address('127.0.0.1')
        self.assertEqual(address, ('127.0.0.1', 8000))
        address = parse_address('127.0.0.1', 8060)
        self.assertEqual(address, ('127.0.0.1', 8060))

    def test_parse_ipv6(self):
        address = parse_address('[::1]')
        self.assertEqual(address, ('::1', 8000))
        address = parse_address('[::1]:8070')
        self.assertEqual(address, ('::1', 8070))

    def test_parse_error(self):
        self.assertRaises(ValueError, parse_address, ())
        self.assertRaises(ValueError, parse_address, (1,))
        self.assertRaises(ValueError, parse_address, (1, 2, 3))
        self.assertRaises(ValueError, parse_address, '127.0.0.1:bla')


class TestParseConnectionString(unittest.TestCase):

    def test_parse_tcp(self):
        scheme, address, params = parse_connection_string('127.0.0.1:8050')
        self.assertEqual(scheme, '')
        self.assertEqual(address, ('127.0.0.1', 8050))
        self.assertEqual(params, {})

    def test_parse_tcp_default(self):
        scheme, address, params = parse_connection_string('127.0.0.1', 8095)
        self.assertEqual(scheme, '')
        self.assertEqual(address, ('127.0.0.1', 8095))
        self.assertEqual(params, {})

    def test_parse_unix(self):
        scheme, address, params = parse_connection_string('unix:bla.foo')
        self.assertEqual(scheme, '')
        self.assertEqual(address, 'bla.foo')
        self.assertEqual(params, {})

    def test_parse_unix_with_scheme(self):
        scheme, address, params = parse_connection_string(
            'redis://unix:bla.foo')
        self.assertEqual(scheme, 'redis')
        self.assertEqual(address, 'bla.foo')
        self.assertEqual(params, {})

    def test_parse_tcp_with_scheme_and_params(self):
        scheme, address, params = parse_connection_string('redis://:6439?db=3')
        self.assertEqual(scheme, 'redis')
        self.assertEqual(address, ('', 6439))
        self.assertEqual(params, {'db': '3'})

    def test_parse_tcp_with_http_and_params(self):
        scheme, address, params = parse_connection_string(
                                                'http://:6439?db=3&bla=foo')
        self.assertEqual(scheme, 'http')
        self.assertEqual(address, ('', 6439))
        self.assertEqual(params, {'db': '3', 'bla': 'foo'})

    def test_parse_tcp_with_https_and_params(self):
        scheme, address, params = parse_connection_string(
                                        'https://127.0.0.1:6439?db=3&bla=foo')
        self.assertEqual(scheme, 'https')
        self.assertEqual(address, ('127.0.0.1', 6439))
        self.assertEqual(params, {'db': '3', 'bla': 'foo'})


class TestMisc(unittest.TestCase):

    def test_socketpair(self):
        server, client = socketpair()
        self.assertEqual(client.send(b'ciao'), 4)
        self.assertEqual(server.recv(io.DEFAULT_BUFFER_SIZE), b'ciao')
        self.assertEqual(server.send(b'ciao a te'), 9)
        self.assertEqual(client.recv(io.DEFAULT_BUFFER_SIZE), b'ciao a te')
        close_socket(server)
        self.assertTrue(is_socket_closed(server))
        self.assertTrue(is_socket_closed(None))

    def test_close_socket(self):
        close_socket(None)
        sock = mock.Mock()
        sock.configure_mock(**{'shutdown.side_effect': TypeError,
                               'close.side_effect': TypeError})
        close_socket(sock)
        sock.shutdown.assert_called_with(socket.SHUT_RDWR)
        sock.close.assert_called_with()

    def test_format_address(self):
        self.assertRaises(ValueError, format_address, (1,))
        self.assertRaises(ValueError, format_address, (1, 2, 3))
        self.assertRaises(ValueError, format_address, (1, 2, 3, 4, 5))
        self.assertEqual(format_address(1), '1')
