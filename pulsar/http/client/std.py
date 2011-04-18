'''\
The Standard Library Http Client

This is a thin layer on top of urllib2 in python2 / urllib in Python 3
It exposes the httplib1 class from the standard library.
'''
import pulsar
if pulsar.ispy3k:
    # Python 3
    from urllib.request import Request, build_opener, install_opener
    from urllib.request import HTTPCookieProcessor, HTTPPasswordMgrWithDefaultRealm
    from urllib.request import HTTPBasicAuthHandler, ProxyHandler
    from urllib.request import getproxies_environment
    from urllib.parse import urlencode
else:
    # Python 2.*
    from urllib2 import Request, build_opener, install_opener, HTTPCookieProcessor
    from urllib2 import HTTPPasswordMgrWithDefaultRealm, HTTPBasicAuthHandler
    from urllib2 import ProxyHandler
    from urllib import urlencode, getproxies_environment

    
    
class Response(object):
    
    def __init__(self, response):
        self.response = response
        
    @property
    def status(self):
        return self.response.code
    
    @property
    def reason(self):
        return self.response.reason
    
    @property
    def content(self):
        if not hasattr(self,'_content'):
            self._content = self.response.read()
        return self._content
    

class HttpClientBase(object):
    DEFAULT_HEADERS = {'user-agent': pulsar.SERVER_SOFTWARE}
    
    def headers(self, headers):
        d = self.DEFAULT_HEADERS.copy()
        if not headers:
            return d
        else:
            d.update(headers)
        return d
        
    
class HttpClient1(HttpClientBase):
    
    def __init__(self, proxy_info = None,
                 timeout = None, cache = None, headers = None):
        proxy = ProxyHandler(proxy_info)
        self._opener = build_opener(proxy)
        self.timeout = timeout
        
    def request(self, url, body=None, **kwargs):
        response = self._opener.open(url,data=body,timeout=self.timeout)
        return Response(response)
    
    def add_password(self, username, password, uri, realm=None):
        '''Add Basic HTTP Authentication to the opener'''
        if realm is None:
            password_mgr = HTTPPasswordMgrWithDefaultRealm()
        else:
            password_mgr = HTTPPasswordMgr()
        password_mgr.add_password(realm, uri, user, passwd)
        self._opener.add_handler(HTTPBasicAuthHandler(password_mgr))
        
        

    
    
