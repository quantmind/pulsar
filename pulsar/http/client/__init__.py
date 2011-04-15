import urllib

from .std import

HttpClients={1:HttpClient1}


def getproxy(schema = 'http'):
    p = urllib.getproxies_environment()
    return p.get(schema,None)


def HttpClient(cache = None, proxy_info = None, timeout = None, type = 1, async = False):
    '''Create a http client handler:
    
    * *cache* is the http cache file.
    * *proxy_info* proxy server
    * *timeout*
    * *type* the type of client.
    '''
    if type not in HttpClients:
        raise ValueError('HttpClient{0} not available'.format(type))
    client = HttpClients[type]
    proxy = proxy_info
    if proxy is None:
        proxy = getproxy()
        
    return client(proxy_info = proxy, cache = cache, timeout = timeout)

    
