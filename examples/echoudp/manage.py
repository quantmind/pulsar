'''
This example illustrates how to write a UDP Echo server and client pair.
The code for this example is located in the :mod:`examples.echoudp.manage`
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
it subclass the handy :class:`.AbstractUdpClient` and uses
the asynchronous :class:`.Pool` of connections as backbone.

The second step is the implementation of the :class:`EchoUdpProtocol`,
a subclass of :class:`.DatagramProtocol`.
The :class:`EchoUdpProtocol` is needed for two reasons:

* It encodes and sends the request to the remote server
* It listens for incoming data from the remote server via the
  :meth:`~EchoUdpProtocol.datagram_received` method.



Implementation
==================

Echo Udp Protocol
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: EchoUdpProtocol
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
import pulsar
from pulsar import Pool, Future, DatagramProtocol
from pulsar.utils.pep import to_bytes
from pulsar.apps.socket import UdpSocketServer


class EchoUdpProtocol(DatagramProtocol):
    '''A base :class:`.DatagramProtocol` for UDP echo clients and servers.

    The only difference between client and server is the implementation
    of the :meth:`response` method.
    '''
    separator = b'\r\n\r\n'
    '''A separator for messages.'''
    buffer = None
    '''The buffer for long messages'''

    def popbuffer(self, addr):
        if self.buffer:
            return self.buffer.pop(addr, None)

    def datagram_received(self, data, addr):
        '''Handle data from ``addr``.
        '''
        while data:
            idx = data.find(self.separator)
            if idx >= 0:    # we have a full message
                idx += len(self.separator)
                chunk, data = data[:idx], data[idx:]
                buffer = self.popbuffer(addr)
                self.response(buffer + chunk if buffer else chunk, addr)
            else:
                if self.buffer is None:
                    self.buffer = {}
                if addr in self.buffer:
                    self.buffer[addr] += data
                else:
                    self.buffer[addr] = data

    def response(self, data, addr):
        '''Abstract response handler'''
        raise NotImplementedError


class EchoUdpClientProtocol(EchoUdpProtocol):
    _waiting = None

    def send(self, message):
        assert self._waiting is None
        self._waiting = d = Future(loop=self._loop)
        self._transport.sendto(to_bytes(message)+self.separator)
        return d

    def response(self, data, addr):
        '''Got a full response'''
        d, self._waiting = self._waiting, None
        if d:
            d.set_result(data[:-len(self.separator)])


class EchoUdpServerProtocol(EchoUdpProtocol):
    '''The :class:`EchoUdpProtocol` used by the echo udp :func:`server`.
    '''
    def response(self, data, addr):
        '''Override :meth:`~EchoProtocol.response` method by writing the
        ``data`` received back to the client.
        '''
        self._transport.sendto(data, addr)


class Echo(pulsar.AbstractUdpClient):
    '''A client for the echo server.

    :param address: set the :attr:`address` attribute
    :param pool_size: used when initialising the connetion :attr:`pool`.
    :param loop: Optional event loop to set the :attr:`_loop` attribute.

    .. attribute:: _loop

        The event loop used by the client IO requests.

        The event loop is stored at this attribute so that asynchronous
        method decorators such as :func:`.task` can be used.

    .. attribute:: address

        remote server UDP address.

    .. attribute:: pool

        Asynchronous client protocol :class:`.Pool`.
    '''
    protocol_factory = EchoUdpClientProtocol

    def __init__(self, address, pool_size=5, loop=None):
        super().__init__(loop)
        self.address = address
        self.pool = Pool(self.create_endpoint, pool_size, self._loop)

    def create_endpoint(self):
        return self.create_datagram_endpoint(remote_addr=self.address)

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
        protocol = await self.pool.connect()
        with protocol:
            result = await protocol.send(message)
            return result


def server(name=None, description=None, **kwargs):
    '''Create the :class:`.UdpSocketServer`.
    '''
    name = name or 'echoudpserver'
    description = description or 'Echo Udp Server'
    return UdpSocketServer(EchoUdpServerProtocol, name=name,
                           description=description, **kwargs)


if __name__ == '__main__':  # pragma nocover
    server().start()
