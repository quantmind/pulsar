'''tests the httpurl stand-alone script.'''
from pulsar.utils.test import test
from pulsar.utils import httpurl

BIN_HOST = 'httpbin.org'
HTTPBIN_URL = 'http://' + BIN_HOST + '/'
HTTPSBIN_URL = 'https://'+ BIN_HOST + '/'


class TestHeaders(test.TestCase):
    
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
        
        
def httpbin(*suffix):
    """Returns url for HTTPBIN resource."""
    return HTTPBIN_URL + '/'.join(suffix)


def httpsbin(*suffix):
    """Returns url for HTTPSBIN resource."""
    return HTTPSBIN_URL + '/'.join(suffix)


class TestHttpClient(test.TestCase):
    
    def setUp(self):
        proxy = self.worker.cfg.http_proxy
        proxy_info = {}
        if proxy:
            proxy_info['http'] = proxy
        self.r = httpurl.HttpClient(proxy_info=proxy_info)
         
    def test_http_200_get(self):
        r = self.r.get(httpbin())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        self.assertTrue(r.content)
        self.assertEqual(r.url, httpbin())
        
    def test_http_200_get_data(self):
        r = self.r.get(httpbin(httpurl.iri_to_uri('get',{'bla':'foo'})))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        result = r.content_json()
        self.assertEqual(result['args'], {'bla':'foo'})
        self.assertEqual(r.url, httpbin(httpurl.iri_to_uri('get',{'bla':'foo'})))
        
    def test_http_400_get(self):
        '''Bad request 400'''
        r = self.r.get(httpbin('status', '400'))
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.response, 'Bad Request')
        self.assertEqual(r.content,b'')
        self.assertRaises(r.HTTPError, r.raise_for_status)
        
    def test_http_404_get(self):
        '''Not Found 404'''
        r = self.r.get(httpbin('status', '404'))
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.response, 'Not Found')
        self.assertEqual(r.content,b'')
        self.assertRaises(r.HTTPError, r.raise_for_status)
        
    def test_http_post(self):
        data = {'bla': 'foo', 'unz': 'whatz', 'numero': '1'}
        r = self.r.post(httpbin('post'), body=data)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.response, 'OK')
        result = r.content_json()
        self.assertEqual(result['form'], data)
        
