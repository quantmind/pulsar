import unittest

try:
    import requests
    http = requests.session()
except ImportError:
    http = None

from pulsar.apps.test import dont_run_with_thread


class TestRequest:

    async def setUp(self):
        http = self.client()
        response = await http.get(self.httpbin())
        self.assertEqual(str(response), '<Response [200]>')

    @dont_run_with_thread
    @unittest.skipUnless(http, "requires python requests library")
    def test_requests_get_200(self):
        response = http.get(self.httpbin(), verify=False,
                            proxies=self.proxies())
        self.assertEqual(str(response), '<Response [200]>')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.content)
        self.assertEqual(response.url, self.httpbin())
        response = http.get(self.httpbin('get'), verify=False,
                            proxies=self.proxies())
        self.assertEqual(response.status_code, 200)
