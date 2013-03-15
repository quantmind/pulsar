import socket

import pulsar
from pulsar import is_failure
from pulsar.utils.pep import to_bytes, to_string
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread

from examples.echo.manage import server, Echo

    
class SafeCallback(pulsar.Deferred):
    
    def __call__(self):
        try:
            r = self._call()
        except Exception as e:
            r = e
        if pulsar.is_async(r):
            return r.add_callback(self)
        else:
            return self.callback(r)
        
    def _call(self):
        raise NotImplementedError()
    

class TestPulsarStreams(unittest.TestCase):
    concurrency = 'thread'
    server = None
    
    @classmethod
    def setUpClass(cls):
        s = server(name=cls.__name__.lower(), bind='127.0.0.1:0',
                   concurrency=cls.concurrency)
        cls.server = yield pulsar.send('arbiter', 'run', s)
        cls.client = Echo(cls.server.address, force_sync=True)
        
    @classmethod
    def tearDownClass(cls):
        if cls.server:
            yield pulsar.send('arbiter', 'kill_actor', cls.server.name)
        
    @run_on_arbiter
    def testServer(self):
        app = pulsar.get_application(self.__class__.__name__.lower())
        self.assertTrue(app.address)
        
        
@dont_run_with_thread
class TestPulsarStreamsProcess(TestPulsarStreams):
    impl = 'process'        

    