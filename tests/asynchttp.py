'''Tests asynchronous HttpClient.'''
from pulsar import make_async

from . import httpurl

class TestAsyncHttpClient(httpurl.TestHttpClient):
    timeout = 0
    
    def test_stream_response(self):
        http = self.client(stream=True)
        self.assertTrue(http.stream)
        r = make_async(http.get(self.httpbin('stream/3000/20')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.sock.closed)
        
        
class TestAsyncHttpClientWithProxy(TestAsyncHttpClient):
    with_proxy = True