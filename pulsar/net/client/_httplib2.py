'''\
The Httplib2 clinet

This is a thin layer on top of httplib2 python library.

http://code.google.com/p/httplib2/
'''
import httplib2


from .std import HttpClient


class Response(object):
    __slots__ = ('response','content')
    def __init__(self, response, content):
        self.response = response
        self.content = content

    
class HttpClient2(HttpClient):
    
    def __init__(self, proxy_info = None,
                 timeout = None, cache = None, headers = None):
        self._opener = httplib2.Http(cache = cache,
                                     timeout = timeout,
                                     proxy_info = proxy_info)
        
    def request(self, uri, method='GET', body=None, headers = None, **kwargs):
        r = self._opener.open(uri,
                              method=method,
                              body=body,
                              headers=self.headers(headers))
        return Response(r)
    
    def add_credentials(self, username, password, domain = ''):
        self._opener.add_credentials(username, password, domain)

