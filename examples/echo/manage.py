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

Echo Client
==================

.. autoclass:: Echo
   :members:
   :member-order: bysource

Run The example
====================

To run the server::

    python manage.py
    
Open a new shell, in this directory, launch python and type::

    >>> from manage import Echo
    >>> p = Echo('localhost:8060', force_sync=True)
    
The `force_sync` set to ``True``, force the client to wait for results rather
than returning a :class:`pulsar.Deferred`.
Check the :ref:`creating synchronous clients <tutorials-synchronous>` tutorial
for further information.

    >>> p.request('Hello')
    'Hello'
'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
import pulsar
from pulsar.apps.socket import SocketServer


class EchoProtocol(pulsar.ProtocolConsumer):
    '''A symmetric echo protocol consumer for client and servers.
The only difference between client and server is the implementation of the
:meth:`response` method.'''
    separator = b'\r\n\r\n'
    '''A separator for messages.'''
    
    def __init__(self, *args, **kwargs):
        super(EchoProtocol, self).__init__(*args, **kwargs)
        self.buffer = b''        
    
    def data_received(self, data):
        '''Implements the :meth:`pulsar.Protocol.data_received` method.
It simply search for the :attr:`separator` and, if found, it invokes the
:meth:`response` method with the value of the message.'''
        idx = data.find(self.separator)
        if idx >= 0: # we have a full message
            idx += len(self.separator)
            data, rest = data[:idx], data[idx:]
            data = self.buffer + data
            self.buffer = b''
            self.finished(self.response(data))
            return rest
        else:
            self.buffer += data
    
    def start_request(self):
        self.transport.write(self.current_request.message + self.separator)
        
    def response(self, data):
        '''Clients return the message so that the
:attr:`pulsar.ProtocolConsumer.on_finished` deferred is called back with the
message value, while servers sends the message back to the client.'''
        return data[:-len(self.separator)]
        

class EchoServerProtocol(EchoProtocol):
    
    def response(self, data):
        # write it back.
        self.transport.write(data)
        # If we get a QUIT message, close the transport. Used by the test suite.
        if data[:-len(self.separator)] == b'QUIT':
            self.transport.close()
    
    
class Echo(pulsar.Client):
    '''Echo client'''
    consumer_factory = EchoProtocol

    def __init__(self, address, full_response=False, **params):
        super(Echo, self).__init__(**params)
        self.full_response = full_response
        self.address = address

    def request(self, message):
        '''Build the client request and return a :class:`pulsar.Deferred`
which will be called once the server has sent back its response.'''
        request = pulsar.Request(self.address, self.timeout)
        request.message = message
        response = self.response(request)
        if self.full_response:
            return response
        elif response.on_finished.done():
            return response.on_finished.result
        else:
            return response.on_finished
            
        

def server(description=None, **kwargs):
    description = description or 'Echo Server'
    return SocketServer(callable=EchoServerProtocol,
                        description=description,
                        **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()