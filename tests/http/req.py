import requests

from pulsar.apps.test import dont_run_with_thread


class TestRequest:
    session = requests.session()

    @dont_run_with_thread
    def test_requests_get_200(self):
        http = self.session
        response = http.get(self.httpbin(), verify=False,
                            proxies=self.proxies())
        self.assertEqual(str(response), '<Response [200]>')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.content)
        self.assertEqual(response.url, self.httpbin())
        response = http.get(self.httpbin('get'), verify=False,
                            proxies=self.proxies())
        self.assertEqual(response.status_code, 200)
