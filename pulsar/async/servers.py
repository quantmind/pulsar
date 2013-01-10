from functools import partial

from pulsar import create_socket
from pulsar.utils.pep import get_event_loop, set_event_loop

from .eventloop import IOLoop
from .access import PulsarThread
from .tcp import TCPServer 

__all__ = ['create_server']

def create_server(actor, sock=None, address=None, backlog=1024,
                  name=None, call_soon=None, **params):
    '''Create a server for an actor'''
    sock = create_socket(sock=sock, address=address, bindto=True,
                         backlog=backlog)
    if sock.type == 'tcp' or sock.type == 'unix':
        server_type = TCPServer
    else:
        raise NotImplemented
    server = server_type(**params)
    requestloop = actor.requestloop
    if requestloop.cpubound:
        # create a server on a different thread
        requestloop.call_soon_threadsafe(_start_on_thread, name, server, sock,
                                         call_soon)
    else:
        _start_server(requestloop, server, sock, call_soon, False)
    return server

################################################################################
##    INTERNALS
def _start_on_thread(name, *args):
    # we are on the actor request loop thread, therefore the event loop
    # should be already available if the tne actor is not CPU bound.
    event_loop = get_event_loop()
    if event_loop is None:
        event_loop = IOLoop()
        set_event_loop(event_loop)
        args = (event_loop,) + args + (True,)
        PulsarThread(name=name, target=start_server, args=args).start()
    else:
        _start_server(event_loop, *args)
    
def _start_server(event_loop, server, sock, call_soon, run):
    server.create_transport(event_loop, sock)
    if call_soon:
        event_loop.call_soon_threadsafe(call_soon)
    if run:
        event_loop.run()