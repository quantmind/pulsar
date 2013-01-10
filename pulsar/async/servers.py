from functools import partial

from pulsar import create_socket

from .eventloop import IOLoop
from .access import PulsarThread
from .tcp import TCPServer 

__all__ = ['create_server']

def create_server(actor, sock=None, address=None, backlog=1024,
                  onthread=False, name=None, call_soon=None,
                  **params):
    '''Create a server for an actor'''
    sock = create_socket(sock=sock, address=address, bindto=True,
                         backlog=backlog)
    if sock.type == 'tcp' or sock.type == 'unix':
        server_type = TCPServer
    else:
        raise NotImplemented
    server = server_type(**params)
    requestloop = actor.requestloop
    # create a server on a different thread
    if onthread:
        requestloop.call_soon(_start_on_thread, name, server, sock, call_soon)
    else:
        _start_server(requestloop, server, sock, call_soon)
    return server
        
def _start_on_thread(name, *args):
    event_loop = (IOLoop(),) + args
    thread = PulsarThread(name=name, target=start_server, args=args)
    
def _start_server(event_loop, server, sock, call_soon):
    server.create_transport(event_loop, sock)
    if call_soon:
        event_loop.call_soon(call_soon)
    if not event_loop.running:
        event_loop.run()