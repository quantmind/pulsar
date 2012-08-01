'''Tests stopping arbiter.'''
import os

import threading

import pulsar
from pulsar.apps.test import unittest, arbiter_test, halt_server,\
                                create_test_arbiter
        
    
class TestArbiter(unittest.TestCase):

    @arbiter_test
    def testMockArbiter(self):
        a = self.arbiter
        self.assertTrue(isinstance(a, pulsar.Arbiter))
        self.assertEqual(threading.current_thread().ident, a.tid)
        self.assertEqual(threading.current_thread().name, 'Mock arbiter thread')
        self.assertEqual(a.impl, 'monitor')
        self.assertTrue(a.running())
        
    def testHaltServer(self):
        a = create_test_arbiter()
        # put a callback to halt the (mock) arbiter
        actor = a.spawn(impl='thread').wait(0.5)
        self.assertEqual(actor.address, None)
        a.ioloop.add_callback(halt_server)
        while a.running():
            yield pulsar.NOT_DONE
        # After this the halt_server callback should have been called
        self.assertTrue(a.stopped())
        