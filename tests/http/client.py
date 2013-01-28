'''Tests asynchronous HttpClient.'''
from pulsar import send, make_async, safe_async, is_failure
from pulsar.apps.test import unittest
from pulsar.utils import httpurl
from pulsar.utils.httpurl import to_bytes, urlencode
from pulsar.apps.wsgi import HttpClient


class TestHttpClientBase(unittest.TestCase):
    app = None
    with_proxy = False
    proxy_app = None
    timeout = 10
    
    @classmethod
    def setUpClass(cls):
        # Create the HttpBin server by sending this request to the arbiter
        from examples.proxyserver.manage import server as pserver
        from examples.httpbin.manage import server
        concurrency = cls.cfg.concurrency
        s = server(bind='127.0.0.1:0', concurrency=concurrency,
                   name='httpbin-%s' % cls.__name__.lower())
        outcome = send('arbiter', 'run', s)
        yield outcome
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        if cls.with_proxy:
            s = pserver(bind='127.0.0.1:0', concurrency=concurrency,
                        name='proxyserver-%s' % cls.__name__.lower())
            outcome = send('arbiter', 'run', s)
            yield outcome
            cls.proxy_app = outcome.result
            cls.proxy_uri = 'http://{0}:{1}'.format(*cls.proxy_app.address)
        
    @classmethod
    def tearDownClass(cls):
        if cls.app is not None:
            yield send('arbiter', 'kill_actor', cls.app.name)
        if cls.proxy_app is not None:
            yield send('arbiter', 'kill_actor', cls.proxy_app.name)
        
    def client(self, **kwargs):
        kwargs['timeout'] = self.timeout
        if self.with_proxy:
            kwargs['proxy_info'] = {'http': self.proxy_uri}
        return HttpClient(**kwargs)
    
    def httpbin(self, *suffix):
        if suffix:
            return self.uri + '/' + '/'.join(suffix)
        else:
            return self.uri
    
    
class TestHttpClient(TestHttpClientBase):
    
    def testClient(self):
        http = self.client(max_redirects=5)
        self.assertTrue('accept-encoding' in http.headers)
        self.assertEqual(http.timeout, self.timeout)
        self.assertEqual(http.version, 'HTTP/1.1')
        self.assertEqual(http.max_redirects, 5)
        if self.with_proxy:
            self.assertEqual(http.proxy_info, {'http': self.proxy_uri})
        
    def test_200_get(self):
        http = self.client()
        response = http.get(self.httpbin())
        yield response.on_finished
        self.assertEqual(str(response), '200 OK')
        self.assertEqual(repr(response), 'HttpResponse(200 OK)')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        self.assertTrue(response.content)
        self.assertEqual(response.url, self.httpbin())