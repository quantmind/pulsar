from .std import HttpClient1, getproxies_environment
HttpClients={1:HttpClient1}
try:
    from ._httplib2 import HttpClient2
    HttpClients[2] = HttpClient2
except ImportError:
    pass


form_headers = {'Content-type': 'application/x-www-form-urlencoded'}


def HttpClient(cache = None, proxy_info = None, timeout = None, type = 1, async = False):
    '''Create a http client handler using different implementation.
It can build a synchronous or an asyncronous handler build on top
of the :class:`pulsar.IOLoop`. 
    
:parameter cache: Cache file. Default ``None``.
:parameter proxy_info: Dictionary of proxies. Default ``None``.
:parameter timeout: Connection timeout. Default ``None``.
:parameter type: Handler implementation. Default ``1``.
:parameter async: Synchronous or Asynchronous. Default ``False``.
'''
    if type not in HttpClients:
        raise ValueError('HttpClient{0} not available'.format(type))
    client = HttpClients[type]
    proxy = proxy_info
    if proxy is None:
        proxy = getproxies_environment()
        
    return client(proxy_info = proxy, cache = cache, timeout = timeout)

    
