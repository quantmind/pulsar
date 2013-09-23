'''test classes for socket clients.'''
import socket

from pulsar import Deferred
from pulsar.apps.test import unittest
from pulsar.utils.pep import get_event_loop

from examples.echo.manage import Echo


def safe(callable, done):
    try:
        yield callable()
    except Exception as e:
        done.callback(e)
    else:
        done.callback(None)
         

class TestClients(unittest.TestCase):
    
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
        result = client.request(address, b'Hello!')
        self.assertFalse(result.connection)
        try:
            yield result.on_finished
        except socket.error as e:
            pass
        self.assertEqual(len(client.connection_pools), 1)
        
    def test_bad_request_in_thread(self):
        address = ('127.0.0.1', 9099)
        client = Echo(full_response=True)
        done = Deferred()
        def _test():
            result = client.request(address, b'Hello!')
            connection = result.connection
            self.assertTrue(connection)
            self.assertEqual(connection.session, 1)
            self.assertEqual(connection.current_consumer, result)
            self.assertEqual(connection.producer, client)
            try:
                yield result.on_finished
            except socket.error as e:
                pass
            self.assertEqual(len(client.connection_pools), 1)
        
        client.get_event_loop().call_soon_threadsafe(safe, _test, done)
        return done