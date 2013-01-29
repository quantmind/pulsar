'''Tests asynchronous HttpClient.'''
from pulsar import send
from pulsar.apps.test import unittest
from pulsar.utils import httpurl
from pulsar.utils.httpurl import to_bytes, urlencode
from pulsar.apps.http import HttpClient


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
        
    def test_200_get_data(self):
        http = self.client()
        response = http.get(self.httpbin('get',''), data={'bla':'foo'})
        yield response.on_finished
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        result = response.content_json()
        self.assertEqual(result['args'], {'bla':['foo']})
        self.assertEqual(response.url,
                self.httpbin(httpurl.iri_to_uri('get/',{'bla':'foo'})))
        
    def test_200_gzip(self):
        http = self.client()
        response = http.get(self.httpbin('gzip'))
        yield response.on_finished
        headers = response.headers
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        content = response.content_json()
        self.assertTrue(content['gzipped'])
        self.assertTrue(response.headers['content-encoding'],'gzip')
        
    def test_400_get(self):
        '''Bad request 400'''
        http = self.client()
        response = http.get(self.httpbin('status', '400'))
        yield response.on_finished
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.response, 'Bad Request')
        self.assertTrue(response.content)
        self.assertRaises(httpurl.HTTPError, response.raise_for_status)
        
    def test_404_get(self):
        '''Not Found 404'''
        http = self.client()
        response = http.get(self.httpbin('status', '404'))
        yield response.on_finished
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.response, 'Not Found')
        self.assertTrue(response.content)
        self.assertRaises(httpurl.HTTPError, response.raise_for_status)
        
    def test_post(self):
        data = (('bla', 'foo'), ('unz', 'whatz'),
                ('numero', '1'), ('numero', '2'))
        http = self.client()
        response = http.post(self.httpbin('post'), encode_multipart=False,
                             data=data)
        yield response.on_finished
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        result = response.content_json()
        self.assertTrue(result['args'])
        self.assertEqual(result['args']['numero'],['1','2'])
    
    def test_post_multipart(self):
        data = (('bla', 'foo'), ('unz', 'whatz'),
                ('numero', '1'), ('numero', '2'))
        http = self.client()
        response = http.post(self.httpbin('post'), data=data)
        yield response.on_finished
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        result = response.content_json()
        self.assertTrue(result['args'])
        self.assertEqual(result['args']['numero'],['1','2'])
        
    def test_put(self):
        data = (('bla', 'foo'), ('unz', 'whatz'),
                ('numero', '1'), ('numero', '2'))
        http = self.client()
        response = http.put(self.httpbin('put'), data=data)
        yield response.on_finished
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        result = response.content_json()
        self.assertTrue(result['args'])
        self.assertEqual(result['args']['numero'],['1','2'])
        
    def test_patch(self):
        data = (('bla', 'foo'), ('unz', 'whatz'),
                ('numero', '1'), ('numero', '2'))
        http = self.client()
        response = http.patch(self.httpbin('patch'), data=data)
        yield response.on_finished
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        result = response.content_json()
        self.assertTrue(result['args'])
        self.assertEqual(result['args']['numero'],['1','2'])
        
    def test_delete(self):
        data = (('bla', 'foo'), ('unz', 'whatz'),
                ('numero', '1'), ('numero', '2'))
        http = self.client()
        response = http.delete(self.httpbin('delete'), data=data)
        yield response.on_finished
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.response, 'OK')
        result = response.content_json()
        self.assertTrue(result['args'])
        self.assertEqual(result['args']['numero'],['1','2'])