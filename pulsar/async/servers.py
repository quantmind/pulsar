from functools import partial

from pulsar import create_socket

from .eventloop import IOLoop
from .access import PulsarThread
from .tcp import TCPServer 

__all__ = ['create_server']

def create_server(actor, sock=None, address=None, backlog=1024,
                  onthread=False, name=None, protocol=None, **params):
    sock = create_socket(sock=sock, address=address, bindto=True,
                         backlog=backlog)
    requestloop = actor.requestloop
    # create a server on a different thread
    if onthread:
        request = requestloop.call_soon(start_on_thread, sock, name, protocol,
                                        params)
    else:
        return start_server(requestloop, sock, protocol, params)
        
def start_on_thread(sock, name, protocol, params):
    event_loop = IOLoop()
    thread = PulsarThread(name=name, target=start_server,
                    args=(event_loop, sock, protocol, params))
    
def start_server(event_loop, sock, protocol, params):
    if sock.type == 'tcp' or sock.type == 'unix':
        server_type = TCPServer
    else:
        raise NotImplemented
    server = server_type(event_loop, sock, protocol, **params)
    event_loop.run()