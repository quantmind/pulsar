import pulsar

from .std import HttpClientHandler, HttpClient1, urlencode,\
                 getproxies_environment, HttpClientResponse,\
                 responses
                 
__all__ = ['HttpClientHandler',
           'HttpClient',
           'HttpClients',
           'HttpClientResponse',
           'setDefaultClient',
           'urlencode',
           'responses']


HttpClients={1:HttpClient1}
try:
    from ._httplib2 import HttpClient2
    HttpClients[2] = HttpClient2
except ImportError:
    pass


form_headers = {'Content-type': 'application/x-www-form-urlencoded'}

_DefaultClient = (2,1)


def setDefaultClient(v):
    global _DefaultClient
    _DefaultClient = v


class AsyncHttpClient(object):
    
    def __init__(self, c, ioloop):
        self.client = c
        self.ioloop = ioloop
        
    def request(self, *args, **kwargs):
        return self.ioloop.add_callback(
            lambda : self.client.request(*args, **kwargs)
        )        
        

def HttpClient(cache = None, proxy_info = None,
               timeout = None, type = None, ioloop = None,
               async = False, handle_cookie = False,
               headers = None):
    '''Factory of :class:`HttpClientHandler` instances.
It can build a synchronous or an asynchronous handler build on top
of the :class:`pulsar.IOLoop`. 
    
:parameter cache: Cache file. Default ``None``.
:parameter proxy_info: Dictionary of proxies. Default ``None``.
:parameter timeout: Connection timeout. Default ``None``.
:parameter type: Request handler implementation. This can be an integer or a
    tuple of integers. Default ``(2,1)``.
:parameter async: Synchronous or Asynchronous. Default ``False``.
'''
    type = type or _DefaultClient
    if isinstance(type,int):
        type = (type,)
    ctype = None
    for t in type:
        if t in HttpClients:
            ctype = t
            break
    if not ctype:
        raise ValueError('HttpClients {0} not available'.format(type))
    client = HttpClients[ctype]
    proxy = proxy_info
    if proxy is None:
        proxy = getproxies_environment()
        
    c = client(proxy_info = proxy, cache = cache, timeout = timeout,
               handle_cookie = handle_cookie, headers = headers)
    if async:
        return AsyncHttpClient(c, ioloop = ioloop)
    else:
        return c

    
