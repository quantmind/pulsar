'''\
The Standard Library Http Client

This is a thin layer on top of urllib2 in python2 / urllib in Python 3
It exposes the httplib1 class from the standard library.
'''
import io

import pulsar
from pulsar.utils.http import Headers
from pulsar.utils.py2py3 import to_bytestring
if pulsar.ispy3k:
    # Python 3
    from urllib.request import Request, build_opener, install_opener
    from urllib.request import HTTPCookieProcessor, HTTPPasswordMgrWithDefaultRealm
    from urllib.request import HTTPBasicAuthHandler, ProxyHandler
    from urllib.request import getproxies_environment, URLError, HTTPError
    from urllib.parse import urlencode
    from http.client import responses
    from http.cookiejar import CookieJar
else:
    # Python 2.*
    from urllib2 import Request, build_opener, install_opener, HTTPCookieProcessor
    from urllib2 import HTTPPasswordMgrWithDefaultRealm, HTTPBasicAuthHandler
    from urllib2 import ProxyHandler, URLError, HTTPError
    from urllib import urlencode, getproxies_environment
    from httplib import responses
    from cookielib import CookieJar
    

class HttpClientResponse(object):
    '''Instances of this class are returned from the
:meth:`HttpClientHandler.request` method.

.. attribute:: status_code

    Numeric `status code`_ of the response
    
.. attribute:: url

    Url of request
    
.. attribute:: response

    Status code description
    
.. attribute:: headers

    List of response headers
    
.. attribute:: content

    Body of response
    
.. attribute:: is_error

    Boolean indicating if this is a response error.
    
.. _`status code`: http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
'''
    _resp = None
    status_code = None
    url = None
    HTTPError = HTTPError
    URLError = URLError 
    
    def __str__(self):
        if self.status_code:
            return '{0} {1}'.format(self.status_code,self.response)
        else:
            return '<None>'
    
    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)
    
    @property
    def is_error(self):
        return isinstance(self._resp,Exception)
    
    @property
    def response(self):
        if self.status_code:
            return responses.get(self.status_code)
        
    def raise_for_status(self):
        """Raises stored :class:`HTTPError` or :class:`URLError`,
 if one occured."""
        if self.is_error:
            raise self._resp
    
    
class ResponseStd(HttpClientResponse):
    status_code = None
    
    def __init__(self, response):
        self._resp = response
        self.status_code = getattr(response, 'code', None)
        self.url = getattr(response, 'url', None)
    
    @property
    def headers(self):
        return getattr(self._resp,'headers',None)
    
    @property
    def content(self):
        if not hasattr(self,'_content') and self._resp:
            if hasattr(self._resp,'read'):
                self._content = self._resp.read()
            else:
                self._content = b''
        return getattr(self,'_content',None)
    
    def content_string(self):
        return self.content.decode()
    

class HttpClientHandler(object):
    '''Http client handler.'''
    HTTPError = HTTPError
    URLError = URLError
    DEFAULT_HEADERS = [('User-agent', pulsar.SERVER_SOFTWARE),
                       ('Connection', 'Keep-Alive')]
    
    def get_headers(self, headers):
        d = Headers(self.DEFAULT_HEADERS)
        if headers:
            d.extend(headers)
        return list(d)
    
    def request(self, url, **kwargs):
        '''Constructs and sends a request.

:param url: URL for the request.
:param method: request method, GET, POST, PUT, DELETE.
:param params: (optional) Dictionary or bytes to be sent in the query string for the :class:`Request`.
:param data: (optional) Dictionary or bytes to send in the body of the :class:`Request`.
:param headers: (optional) Dictionary of HTTP Headers to send with the :class:`Request`.
:param cookies: (optional) Dict or CookieJar object to send with the :class:`Request`.
:param files: (optional) Dictionary of 'filename': file-like-objects for multipart encoding upload.
:param auth: (optional) AuthObject to enable Basic HTTP Auth.
:param timeout: (optional) Float describing the timeout of the request.
:param allow_redirects: (optional) Boolean. Set to True if POST/PUT/DELETE redirect following is allowed.
:param proxies: (optional) Dictionary mapping protocol to the URL of the proxy.
:param return_response: (optional) If False, an un-sent Request object will returned.
:return: :class:`HttpClientResponse` object.
'''
        raise NotImplementedError
        
    def get(self, url):
        '''Sends a GET request and returns a :class:`HttpClientResponse`
object.'''
        return self.request(url, method = 'GET')
    
    def post(self, url, body = None):
        return self.request(url, body = body, method = 'POST')
    
    
class HttpClient1(HttpClientHandler):
    '''Http handler from the standard library'''
    type = 1
    def __init__(self, proxy_info = None,
                 timeout = None, cache = None,
                 headers = None, handle_cookie = False):
        handlers = [ProxyHandler(proxy_info)]
        if handle_cookie:
            cj = CookieJar()
            handlers.append(HTTPCookieProcessor(cj))
        self._opener = build_opener(*handlers)
        self._opener.addheaders = self.get_headers(headers)    
        self.timeout = timeout
        
    @property
    def headers(self):
        return self._opener.addheaders
    
    def request(self, url, body=None, **kwargs):
        if body:
            body = to_bytestring(body)
        try:
            req = Request(url, body, dict(self.headers))
            response = self._opener.open(req,timeout=self.timeout)
        except (HTTPError,URLError) as why:
            return ResponseStd(why)
        else:
            return ResponseStd(response)
    
    def add_password(self, username, password, uri, realm=None):
        '''Add Basic HTTP Authentication to the opener'''
        if realm is None:
            password_mgr = HTTPPasswordMgrWithDefaultRealm()
        else:
            password_mgr = HTTPPasswordMgr()
        password_mgr.add_password(realm, uri, user, passwd)
        self._opener.add_handler(HTTPBasicAuthHandler(password_mgr))
        
        

    
    
