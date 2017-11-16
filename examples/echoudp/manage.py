"""
This example illustrates how to write a UDP Echo server and client pair.
The code for this example is located in the :mod:`examples.echoudp.manage`
module.

Run The example
====================

To run the server::

    python manage.py

Open a new shell, in this directory, launch python and type::

    >>> from manage import Echo
    >>> echo = Echo(('localhost', 8060))
    >>> echo(b'Hello!\\n')
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


Echo Server
==================

.. autofunction:: server

"""
from pulsar.api import DatagramProtocol
from pulsar.apps.socket import UdpSocketServer

from examples.echo.manage import EchoServerProtocol, Echo as EchoTcp


class Echo(EchoTcp):
    '''A client for the Echo UDP server.
    '''
    protocol_type = DatagramProtocol

    def connect(self):
        return self.create_datagram_endpoint(remote_addr=self.address)


def server(name=None, description=None, **kwargs):
    '''Create the :class:`.UdpSocketServer`.
    '''
    return UdpSocketServer(
        EchoServerProtocol,
        name=name or 'echoudpserver',
        description=description or 'Echo Udp Server',
        **kwargs
    )


if __name__ == '__main__':  # pragma nocover
    server().start()
