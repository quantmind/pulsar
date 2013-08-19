import os
import socket
import tempfile
import time

import pulsar
from pulsar import platform
from pulsar.utils.internet import parse_address, parse_connection_string
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
        scheme, address, params = parse_connection_string('redis://unix:bla.foo')
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