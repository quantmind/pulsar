'''
This example illustrates how to write a simple TCP Echo server and client pair.
The example is simple because the client and server protocols are symmetrical
and therefore the :class:`EchoProtocol` will also be used as based class for
:class:`EchoServerProtocol`.
The code for this example is located in the :mod:`examples.echo.manage`
module.

Writing the Client
=========================

The protocol consumer
~~~~~~~~~~~~~~~~~~~~~~~~

The first step is to subclass :class:`pulsar.ProtocolConsumer` to create the
:class:`EchoProtocol` used by the client. The :class:`EchoProtocol` is needed
for two reasons: 

* It encodes and sends the request to the remote server via the
  :meth:`EchoProtocol.start_request` method.
* It listens for incoming data from the remote server via the
  :meth:`EchoProtocol.data_received` method.


The client
~~~~~~~~~~~~~~~

Pulsar provides :ref:`additional classes <clients-api>` for writing
clients handling multiple requests. Here we subclass the :class:`pulsar.Client`
and implement the :class:`Echo.request` method. :class:`Echo` is the main
client class, used in all interactions with the echo server::

    >>> client = Echo(('127,0,0,1', 8080))
    >>> response = client.request(b'Hello!')


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

    >>> p.request(b'Hello')
    b'Hello'
    
Implementation
==================

Echo Client Protocol
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EchoProtocol
   :members:
   :member-order: bysource

Echo Server Protocol
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EchoServerProtocol
   :members:
   :member-order: bysource
   
Echo Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Echo
   :members:
   :member-order: bysource
   
Echo Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: server

'''
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
import pulsar
from pulsar.apps.socket import SocketServer


class EchoProtocol(pulsar.ProtocolConsumer):
    '''A symmetric echo :class:`pulsar.ProtocolConsumer` for client and servers.
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
        '''Override :meth:`pulsar.Protocol.start_request` to write
the message ended by the :attr:`separator` into the transport.'''
        self.transport.write(self.current_request.message + self.separator)
        
    def response(self, data):
        '''Clients return the message so that the
:attr:`pulsar.ProtocolConsumer.on_finished` deferred is called back with the
message value, while servers sends the message back to the client.'''
        return data[:-len(self.separator)]
        

class EchoServerProtocol(EchoProtocol):
    '''The :class:`pulsar.ProtocolConsumer` used by the echo :func:`server`.'''
    
    def response(self, data):
        '''Override :meth:`EchoProtocol.response` method by writing the
``data`` received back to the client.'''
        self.transport.write(data)
        # If we get a QUIT message, close the transport. Used by the test suite.
        if data[:-len(self.separator)] == b'QUIT':
            self.transport.close()
    
    
class Echo(pulsar.Client):
    '''Echo :class:`pulsar.Client`.
    
.. attribute:: full_response

    Flag indicating if the :meth:`request` method should return the
    :class:`EchoProtocol` handling the request (``True``) or a :class:`Deferred`
    which will result in the server response message (``False``).
    
    Default: ``False``
'''
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
    '''Create the :class:`pulsar.apps.socket.SocketServer` instance with
:class:`EchoServerProtocol` as protocol factory.'''
    description = description or 'Echo Server'
    return SocketServer(EchoServerProtocol, description=description, **kwargs)
    

if __name__ == '__main__':  #pragma nocover
    server().start()