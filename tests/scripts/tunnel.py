from functools import partial
try:
    from pulsar.apps import http
except ImportError:
    import sys
    sys.path.append('../../')
    from pulsar.apps import http

from pulsar.utils.log import configure_logging

HOME_URL = 'https://127.0.0.1:8060/'

configure_logging(level='debug')
proxy_info = {'http': 'http://127.0.0.1:9080',
              'https': 'http://127.0.0.1:9080'}

def data_received(response, data=None):
    print('DATA RECEIVED')
    print(response.request)
    print('%s' % data)
    print('---------------------------------------------------')
    print('')

def pre_request(response):
    connection = response.connection
    print('PRE REQUEST ON CONNECTION %s' % connection)
    print(response.request)
    print('---------------------------------------------------')
    print('')
    
def post_request(response):
    print('REQUEST DONE')  
    print(response)
    print('Headers')
    print(response.headers)
    return response
    
hooks = {'data_received': data_received,
         'pre_request': pre_request,
         'post_request': post_request}
                                    
client = http.HttpClient(proxy_info=proxy_info, force_sync=True)
def get(bit=''):
    url = HOME_URL + bit
    return client.get(url, **hooks)
print('======================================================================')
print('======================================================================')
print(get())
print('\n\n')
print('======================================================================')
print('======================================================================')
print(get('redirect/1'))
print('\n\n')
print('======================================================================')
print('======================================================================')
print(get('status/400'))