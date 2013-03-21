'''Tests asynchronous HttpClient.'''
from pulsar import is_failure, async
from pulsar.apps.test import unittest
from pulsar.apps.http import HttpClient, URLError


class TestHttpErrors(unittest.TestCase):
    
    @async(max_errors=2)
    def test_bad_host(self):
        client = HttpClient()
        response = client.get('http://xxxyyyxxxxyyy/blafoo')
        result = yield response.on_finished
        self.assertEqual(response.status_code, None)
        self.assertTrue(is_failure(result))
        self.assertTrue(response.is_error)
        self.assertRaises(URLError, response.raise_for_status)
        yield   # To remove the last error
        