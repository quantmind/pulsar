import socket
import unittest
from unittest import mock

from pulsar.utils.internet import (parse_address, parse_connection_string,
                                   close_socket, format_address)


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
