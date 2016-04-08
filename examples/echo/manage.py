'''
This example illustrates how to write a simple TCP Echo server and client pair.
The example is simple because the client and server protocols are symmetrical
and therefore the :class:`EchoProtocol` will also be used as based class for
:class:`EchoServerProtocol`.
The code for this example is located in the :mod:`examples.echo.manage`
module.

Run The example
====================

To run the server::

    python manage.py

Open a new shell, in this directory, launch python and type::

    >>> from manage import Echo
    >>> echo = Echo(('localhost',8060))
    >>> echo(b'Hello!')
    b'Hello!'

Writing the Client
=========================
The first step is to write a small class handling a connection
pool with the remote server. The :class:`Echo` class does just that,
it subclasses the handy :class:`.AbstractClient` and uses
the asynchronous :class:`.Pool` of connections as backbone.

The second step is the implementation of the :class:`.EchoProtocol`,
a subclass of :class:`.ProtocolConsumer`.
The :class:`EchoProtocol` is needed for two reasons:

* It encodes and sends the request to the remote server via the
  :meth:`~EchoProtocol.start_request` method.
* It listens for incoming data from the remote server via the
  :meth:`~EchoProtocol.data_received` method.

To wait for the response message one can ``await`` from the
:attr:`.ProtocolConsumer.on_finished` event.



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

   .. automethod:: __call__

Echo Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: server

'''
from functools import partial

import pulsar
from pulsar import Pool, Connection, AbstractClient, ProtocolError
from pulsar.apps.socket import SocketServer


class EchoProtocol(pulsar.ProtocolConsumer):
    '''An echo :class:`~.ProtocolConsumer` for client and servers.

    The only difference between client and server is the implementation
    of the :meth:`response` method.
    '''
    separator = b'\r\n\r\n'
    '''A separator for messages.'''
    buffer = b''
    '''The buffer for long messages'''

    def data_received(self, data):
        '''Implements the :meth:`~.ProtocolConsumer.data_received` method.

        It simply search for the :attr:`separator` and, if found, it invokes
        the :meth:`response` method with the value of the message.
        '''
        if self.buffer:
            data = self.buffer + data

        idx = data.find(self.separator)
        if idx >= 0:    # we have a full message
            idx += len(self.separator)
            data, rest = data[:idx], data[idx:]
            self.buffer = self.response(data, rest)
            self.finished()
            return rest
        else:
            self.buffer = data

    def start_request(self):
        '''Override :meth:`~.ProtocolConsumer.start_request` to write
        the message ended by the :attr:`separator` into the transport.
        '''
        self.transport.write(self._request + self.separator)

    def response(self, data, rest):
        '''Clients return the message so that the
        :attr:`.ProtocolConsumer.on_finished` is called back with the
        message value, while servers sends the message back to the client.
        '''
        if rest:
            raise ProtocolError
        return data[:-len(self.separator)]


class EchoServerProtocol(EchoProtocol):
    '''The :class:`EchoProtocol` used by the echo :func:`server`.
    '''
    def response(self, data, rest):
        '''Override :meth:`~EchoProtocol.response` method by writing the
        ``data`` received back to the client.
        '''
        self.transport.write(data)
        data = data[:-len(self.separator)]
        # If we get a QUIT message, close the transport.
        # Used by the test suite.
        if data == b'QUIT':
            self.transport.close()
        return data


class Echo(AbstractClient):
    '''A client for the echo server.

    :param address: set the :attr:`address` attribute
    :param full_response: set the :attr:`full_response` attribute
    :param pool_size: used when initialising the connetion :attr:`pool`.
    :param loop: Optional event loop to set the :attr:`_loop` attribute.

    .. attribute:: _loop

        The event loop used by the client IO requests.

        The event loop is stored at this attribute so that asynchronous
        method decorators such as :func:`.task` can be used.

    .. attribute:: address

        remote server TCP address.

    .. attribute:: pool

        Asynchronous connection :class:`.Pool`.

    .. attribute:: full_response

        Flag indicating if the callable method should return the
        :class:`EchoProtocol` handling the request (``True``) or
        the server response message (``False``).

        Default: ``False``
    '''
    protocol_factory = partial(Connection, EchoProtocol)

    def __init__(self, address, full_response=False, pool_size=10, loop=None):
        super().__init__(loop)
        self.address = address
        self.full_response = full_response
        self.pool = Pool(self.connect, pool_size, self._loop)

    def connect(self):
        return self.create_connection(self.address)

    def __call__(self, message):
        '''Send a ``message`` to the server and wait for a response.

        :return: a :class:`.Future`
        '''
        result = self._call(message)
        if not self._loop.is_running():
            return self._loop.run_until_complete(result)
        else:
            return result

    async def _call(self, message):
        connection = await self.pool.connect()
        with connection:
            consumer = connection.current_consumer()
            consumer.start(message)
            await consumer.on_finished
            return consumer if self.full_response else consumer.buffer


def server(name=None, description=None, **kwargs):
    '''Create the :class:`.SocketServer` with :class:`EchoServerProtocol`
    as protocol factory.
    '''
    name = name or 'echoserver'
    description = description or 'Echo Server'
    return SocketServer(EchoServerProtocol, name=name,
                        description=description, **kwargs)


def log_connection(connection, exc=None):
    if not exc:
        connection.logger.info('Got a new connection!')


if __name__ == '__main__':  # pragma nocover
    server(connection_made=log_connection).start()
