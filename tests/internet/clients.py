'''test classes for socket clients.'''
import socket

from pulsar import Deferred
from pulsar.apps.test import unittest
from pulsar.utils.pep import get_event_loop

from examples.echo.manage import Echo, EchoProtocol


def safe(callable, done):
    try:
        yield callable()
    except Exception as e:
        done.callback(e)
    else:
        done.callback(None)


class TestClients(unittest.TestCase):

    def test_meta(self):
        client = Echo()
        self.assertEqual(str(client), 'Echo')
        self.assertEqual(client.max_reconnect, 1)
        client = Echo(max_reconnect=10)
        self.assertEqual(client.max_reconnect, 10)
        self.assertEqual(client.consumer_factory, EchoProtocol)
        client = Echo(consumer_factory=object)
        self.assertEqual(client.consumer_factory, object)
        self.assertRaises(AttributeError, client._response, None, None, None,
                          None, None)

    def test_event_loop(self):
        client = Echo()
        self.assertFalse(client.event_loop)
        self.assertTrue(client.get_event_loop())
        self.assertEqual(client.get_event_loop(), get_event_loop())

    def test_has_event_loop(self):
        loop = get_event_loop()
        client = Echo(event_loop=loop)
        self.assertTrue(client.event_loop)
        self.assertEqual(client.event_loop, loop)
        self.assertEqual(client.get_event_loop(), loop)

    def test_bad_request(self):
        address = ('127.0.0.1', 9099)
        client = Echo()
        try:
            result = yield client.request(address, b'Hello!')
        except socket.error as e:
            pass
        self.assertEqual(len(client.connection_pools), 1)

    def test_bad_request_full_response(self):
        address = ('127.0.0.1', 9099)
        client = Echo(full_response=True)

        def _test_bad_request():
            # Run on client event loop so we test for the existing connection
            result = client.request(address, b'Hello!')
            self.assertTrue(result.connection)
            try:
                yield result.on_finished
            except socket.error as e:
                pass
            self.assertFalse(result.connection.transport)
            self.assertEqual(len(client.connection_pools), 1)

        client.get_event_loop().call_soon(_test_bad_request)
