'''Tests asynchronous HttpClient.'''
import socket

from pulsar import is_failure
from pulsar.apps.test import unittest
from pulsar.apps.http import HttpClient, URLError


class TestHttpErrors(unittest.TestCase):
    
    def test_bad_host(self):
        client = HttpClient()
        response = client.get('http://xxxyyyxxxxyyy/blafoo')
        try:
            yield response.on_finished
        except socket.error:
            pass
        self.assertFalse(response.status_code)
        self.assertTrue(response.is_error)
        self.assertRaises(URLError, response.raise_for_status)
        