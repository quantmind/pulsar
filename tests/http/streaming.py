from . import client

class TestStreamingHttpClient(client.TestHttpClientBase):
    timeout = 0
    
    def test_stream_response(self):
        http = self.client(stream=True)
        self.assertTrue(http.stream)
        future = http.get(self.httpbin('stream/3000/200'))
        yield future
        response = future.result
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.sock.closed)
        self.assertTrue(response.streaming)
        data = list(response.stream())
        self.assertTrue(data)
        
        
class TestStreamingHttpClientWithProxy(TestStreamingHttpClient):
    with_proxy = True