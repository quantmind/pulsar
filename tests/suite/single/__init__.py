'''Test a single test function on a module with only __init__.py'''
import time

from pulsar import send, get_actor
from pulsar.apps.test import unittest


class TestOneAsync(unittest.TestCase):
    
    def test_ping_monitor(self):
        worker = get_actor()
        future = yield send('monitor', 'ping')
        self.assertEqual(future, 'pong')
        yield self.async.assertEqual(send(worker.monitor, 'ping'), 'pong')
        response = yield send('monitor', 'notify', worker.info())
        self.assertTrue(response <= time.time())