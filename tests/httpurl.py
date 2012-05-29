'''tests the httpurl stand-alone script.'''
from pulsar import make_async
from pulsar.apps.test import unittest, test_server
from pulsar.utils import httpurl

from examples.httpbin.manage import server

BIN_HOST = 'httpbin.org'
HTTPBIN_URL = 'http://' + BIN_HOST + '/'
HTTPSBIN_URL = 'https://'+ BIN_HOST + '/'


class TestHeaders(unittest.TestCase):
    
    def testServerHeader(self):
        h = httpurl.Headers()
        self.assertEqual(h.type, 'server')
        self.assertEqual(len(h), 0)
        h['content-type'] = 'text/html'
        self.assertEqual(len(h), 1)
        
    def testClientHeader(self):
        h = httpurl.Headers('client')
        self.assertEqual(h.type, 'client')
        self.assertEqual(len(h), 0)
        h['content-type'] = 'text/html'
        self.assertEqual(len(h), 1)
        h['server'] = 'bla'
        self.assertEqual(len(h), 1)
        
    def testOrder(self):
        h = httpurl.Headers()
        h['content-type'] = 'text/html'
        h['connection'] = 'close'
        self.assertEqual(len(h), 2)
        self.assertEqual(tuple(h),('Connection', 'Content-Type'))
        h.update({'server': 'foo'})
        self.assertEqual(tuple(h),('Connection', 'Server', 'Content-Type'))


def httpsbin(*suffix):
    """Returns url for HTTPSBIN resource."""
    return HTTPSBIN_URL + '/'.join(suffix)


class TestHttpClient(unittest.TestCase):
    HttpClient = httpurl.HttpClient
    
    @classmethod
    def setUpClass(cls):
        s = test_server(server,
                        bind='127.0.0.1:0',
                        concurrency='thread',
                        workers=0)
        outcome = cls.worker.run_on_arbiter(s)
        yield outcome
        app = outcome.result
        cls.app = app
        cls.uri = 'http://{0}:{1}'.format(*app.address)
        
    @classmethod
    def tearDownClass(cls):
        return cls.worker.arbiter.send(cls.worker,'kill_actor',cls.app.mid)
        
    def setUp(self):
        proxy = self.worker.cfg.http_proxy
        proxy_info = {}
        if proxy:
            proxy_info['http'] = proxy
        self.r = self.HttpClient(proxy_info=proxy_info)
        
    def httpbin(self, *suffix):
        if suffix:
            return self.uri + '/' + '/'.join(suffix)
        else:
            return self.uri
 
    def make_async(self, r):
        return make_async(r)
    
    def testClient(self):
        c = self.r
        self.assertTrue('accept-encoding' in c.DEFAULT_HTTP_HEADERS)
        
    def test_http_200_get(self):
        r = self.make_async(self.r.get(self.httpbin()))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        self.assertTrue(r.content)
        self.assertEqual(r.url, self.httpbin())
        
    def test_http_200_get_data(self):
        r = self.make_async(self.r.get(self.httpbin('get'),
                                       body={'bla':'foo'}))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        result = r.content_json()
        self.assertEqual(result['args'], {'bla':['foo']})
        self.assertEqual(r.url,
                         self.httpbin(httpurl.iri_to_uri('get',{'bla':'foo'})))
        
    def test_http_200_gzip(self):
        r = self.make_async(self.r.get(self.httpbin('gzip')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        content = r.content_json()
        self.assertTrue(content['gzipped'])
        self.assertTrue(r.headers['content-encoding'],'gzip')
        
    def test_http_400_get(self):
        '''Bad request 400'''
        r = self.make_async(self.r.get(self.httpbin('status', '400')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.response, 'Bad Request')
        self.assertEqual(r.content,b'')
        self.assertRaises(httpurl.HTTPError, r.raise_for_status)
        
    def test_http_404_get(self):
        '''Not Found 404'''
        r = self.make_async(self.r.get(self.httpbin('status', '404')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.response, 'Not Found')
        self.assertEqual(r.content,b'')
        self.assertRaises(httpurl.HTTPError, r.raise_for_status)
        
    def test_http_post(self):
        data = (('bla', 'foo'), ('unz', 'whatz'),
                ('numero', '1'), ('numero', '2'))
        r = self.make_async(self.r.post(self.httpbin('post'), body=data))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        result = r.content_json()
        self.assertTrue(result['args'])
        self.assertEqual(result['args']['numero'],['1','2'])
        
    def testRedirect(self):
        r = self.make_async(self.r.get(self.httpbin('redirect','1')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 302)
        self.assertEqual(r.response, 'Found')
        self.assertEqual(r.headers['location'], '/')
        
    def test_Cookie(self):
        r = self.make_async(self.r.get(self.httpbin('cookies','set',
                                                    'bla','foo')))
        yield r
        r = r.result
        self.assertEqual(r.status_code, 302)
        location = self.httpbin('cookies')
        self.assertEqual(r.headers['location'], location)
        #r = self.make_async(self.r.get(self.httpbin('cookies')))
        #yield r
        #r = r.result
        #self.assertEqual(r.status_code, 200)
        #result = r.content_json()
        #self.assertEqual(result['cookies']['key'],'bla')
        #self.assertEqual(result['cookies']['value'],'foo')
        
