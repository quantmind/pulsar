'''\
The Standard Library Http Client

This is a thin layer on top of urllib2 in python2 / urllib in Python 3
It exposes the httplib1 class from the standard library.
'''
from pulsar.utils.py2py3 import ispy3k
if ispy3k:
    # Python 3
    from urllib.request import Request, build_opener, install_opener
    from urllib.request import HTTPCookieProcessor, HTTPPasswordMgrWithDefaultRealm
    from urllib.request import HTTPBasicAuthHandler
    from urllib.parse import urlencode
else:
    # Python 2.*
    from urllib2 import Request, build_opener, install_opener, HTTPCookieProcessor
    from urllib2 import HTTPPasswordMgrWithDefaultRealm, HTTPBasicAuthHandler
    from urllib import urlencode
    
    
class Response(object):
    
    def __init__(self, response):
        self._response = response
    
    
class HttpClient1(object):

    def __init__(self, proxy_info = None, timeout = None, cache = None):
        self._opener = opener
        self.timeout = timeout
        
    def request(self, url, body=None, **kwargs):
        response = self._opener.open(url,data=body,timeout=timeout)
        content = c.read()
        return (Response(response),content)
    
    @classmethod
    def auth(cls, username, password, uri, realm=None, timeout=None):
        '''Create an httplib1 instance witn a basic authentication handler.
The authentication'''
        if realm is None:
            password_mgr = HTTPPasswordMgrWithDefaultRealm()
        else:
            password_mgr = HTTPPasswordMgr()
        password_mgr.add_password(realm, uri, user, passwd)
        opener = HTTPBasicAuthHandler(password_mgr)
        return cls(opener,timeout)
        

    
    
