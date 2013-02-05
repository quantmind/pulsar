'''Pulsar provides several classes for writing clients. The first step
is to subclass :class:`pulsar.ProtocolConsumer` which is needed
for two reasons:

* It encodes and sends the request to the remote server.
* It listen (if needed) for incoming data from the remote server.

There are two methods which needs implementing:

* :meth:`pulsar.ProtocolConsumer.start_request` to kick start a request to a
  remote server.
* :meth:`pulsar.Protocol.data_received`, invoked by the
  :class:`pulsar.Connection` when new data has been received from the
  remote server.

In this example we implement a very simple Echo server/client pair. Because
this example has symmetric client and server protocols, the
:class:`EchoProtocol` will also be used for the server.


Echo Protocol
==================

.. autoclass:: EchoProtocol
   :members:
   :member-order: bysource


Run The example
====================

To run the server::

    python manage.py
    
Open a new shell, in this directory, launch python and type::

    >>> from manage import Echo
    >>> p = Echo('localhost:8060')
    >>> response = p.request('Hello')
'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
import pulsar
from pulsar.apps.socket import SocketServer


class EchoProtocol(pulsar.ProtocolConsumer):
    '''Echo protocol consumer for client and servers.'''
    separator = b'\r\n\r\n'
    '''A separator for messages.'''
    
    def data_received(self, data):
        '''Implements the :meth:`pulsar.Protocol.data_received` method.
It simple search for the :attr:`separator` and if found it invokes the
:meth:`response` method with the value of the message.'''
        idx = data.find(self.separator)
        if idx: # we have a full message
            idx += len(self.separator)
            data, rest = data[:idx], data[idx:]
            self.response(data)
            self.finished()
            return rest
    
    def start_request(self):
        self.transport.write(self.current_request.message+self.separator)
        
    def response(self, data):
        '''Clients store the message in the **result** attribute, while servers
sends the message back to the client.'''
        self.result = data[:len(self.separator)]
        

class EchoServerProtocol(EchoProtocol):
    
    def response(self, data):
        # write it back
        self.transport.write(data)
    
    
class Echo(pulsar.Client):
    consumer_factory = EchoProtocol

    def __init__(self, address, **params):
        super(Echo, self).__init__(**params)
        self.address = address

    def request(self, message):
        request = pulsar.Request(self.address, self.timeout)
        request.message = message
        return self.response(request)
        


def server(description=None, **kwargs):
    description = description or 'Echo Server'
    return SocketServer(callable=EchoServerProtocol,
                        description=description,
                        **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()