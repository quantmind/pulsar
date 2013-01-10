'''A very simple Redis proxy in Pulsar::

    python manage.py
    
To see options type::

    python manage.py -h
'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
from pulsar.apps.socket import SocketServer


def redis_proxy(parsed_data):
    pass


def server(description=None, **kwargs):
    description = description or 'Pulsar Redis Server'
    return SocketServer(callable=redis_proxy,
                        description=description,
                        **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()