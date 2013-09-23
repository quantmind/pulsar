import threading
import time

import pulsar
from pulsar import is_failure, get_actor, async_sleep
from pulsar.utils.pep import to_bytes, to_string
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread

from examples.echo.manage import server, Echo


class TestSyncClient(unittest.TestCase):
    server = None
    
    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   concurrency=get_actor().cfg.concurrency)
        cls.server = yield pulsar.send('arbiter', 'run', s)
        cls.client = Echo(force_sync=True)
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
    
    def test_meta(self):
        self.assertTrue(self.client.force_sync)
    
    def test_echo(self):
        echo = self.client.client(self.server.address)
        self.assertEqual(echo(b'ciao!'), b'ciao!')
        self.assertEqual(echo(b'fooooooooooooo!'),  b'fooooooooooooo!')
    
    def test_close(self):
        echo = self.client.client(self.server.address)
        self.assertEqual(echo(b'ciao!'), b'ciao!')
        self.assertEqual(echo(b'QUIT'), b'QUIT')
        yield async_sleep(1)
        self.assertEqual(echo(b'ciao!'), b'ciao!')