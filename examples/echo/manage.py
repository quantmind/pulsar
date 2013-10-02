'''
This example illustrates how to write a simple TCP Echo server and client pair.
The example is simple because the client and server protocols are symmetrical
and therefore the :class:`EchoProtocol` will also be used as based class for
:class:`EchoServerProtocol`.
The code for this example is located in the :mod:`examples.echo.manage`
module.

Writing the Client
=========================

There are two classes one needs to implement in order to have a flexible client
for Echo servers, or for any TCP servers.

The first class implements the :class:`pulsar.ProtocolConsumer` as it is
described in the next session, while the second class implements the
:class:`pulsar.Client` which is a thread safe
pool of connections to remote servers.

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

    >>> pool = Echo()
    >>> echo = pool.client(('127,0,0,1', 8080))
    >>> response = echo(b'Hello!')


Run The example
====================

To run the server::

    python manage.py

Open a new shell, in this directory, launch python and type::

    >>> from manage import Echo
    >>> echo = Echo(force_sync=True).client(('localhost',8060))

The `force_sync` set to ``True``, force the client to wait for results rather
than returning a :class:`pulsar.Deferred`.
Check the :ref:`creating synchronous clients <tutorials-synchronous>` tutorial
for further information.

    >>> echo(b'Hello')
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
from functools import partial

try:
    import pulsar
except ImportError:     # pragma nocover
    import sys
    sys.path.append('../../')
    import pulsar

from pulsar.apps.socket import SocketServer


class EchoProtocol(pulsar.ProtocolConsumer):
    '''An echo :class:`pulsar.ProtocolConsumer` for client and servers.

    The only difference between client and server is the implementation
    of the :meth:`response` method.
    '''
    separator = b'\r\n\r\n'
    '''A separator for messages.'''
    buffer = b''
    '''The buffer for long messages'''

    def data_received(self, data):
        '''Implements the :meth:`pulsar.Protocol.data_received` method.

        It simply search for the :attr:`separator` and, if found, it invokes
        the :meth:`response` method with the value of the message.
        '''
        idx = data.find(self.separator)
        if idx >= 0:    # we have a full message
            idx += len(self.separator)
            data, rest = data[:idx], data[idx:]
            self.buffer = self.response(self.buffer+data)
            self.finished()
            return rest
        else:
            self.buffer = self.buffer + data

    def start_request(self):
        '''Override :meth:`pulsar.Protocol.start_request` to write
the message ended by the :attr:`separator` into the transport.'''
        self.transport.write(self.request.message + self.separator)

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
        data = data[:-len(self.separator)]
        # If we get a QUIT message, close the transport.
        # Used by the test suite.
        if data == b'QUIT':
            self.transport.close()
        return data


class Echo(pulsar.Client):
    '''Echo :class:`pulsar.Client`.

    .. attribute:: full_response

        Flag indicating if the :meth:`request` method should return the
        :class:`EchoProtocol` handling the request (``True``) or a
        :class:`Deferred` which will result in the server response message
        (``False``).

        Default: ``False``
    '''
    consumer_factory = EchoProtocol

    def setup(self, full_response=False, **params):
        self.full_response = full_response

    def client(self, address):
        '''Utility for returning a function which interact with one server.
        '''
        return partial(self.request, address)

    def request(self, address, message):
        '''Build the client request send it to the server.
        '''
        request = pulsar.Request(address, self.timeout)
        request.message = message
        response = self.response(request)
        if self.full_response:
            return response
        elif response.on_finished.done():
            return response.buffer
        else:
            return response.on_finished.add_callback(lambda r: r.buffer)


def server(description=None, **kwargs):
    '''Create the :class:`pulsar.apps.socket.SocketServer` instance with
:class:`EchoServerProtocol` as protocol factory.'''
    description = description or 'Echo Server'
    return SocketServer(EchoServerProtocol, description=description, **kwargs)


if __name__ == '__main__':  # pragma nocover
    server().start()
