'''Tests for arbiter and monitors.'''
import os
from threading import current_thread

import pulsar
from pulsar import send, spawn
from pulsar.apps.test import unittest, run_on_arbiter


class ActorA(pulsar.Actor):
    '''This actor ping application "app1" and once it receive the callback
it fires a message with the result to its monitor.'''
    def on_task(self):
        '''Ping the application monitor "app1"'''
        if not hasattr(self,'_result'):
            self._result = None
            self.log.debug('{0} pinging "app1"'.format(self))
            server = self.ACTOR_LINKS["app1"]
            self.proxy.ping(server).add_callback(self.fire_result)
        
    def fire_result(self, result):
        self.monitor.send(self.aid,result)
    
    
class TestArbiter(unittest.TestCase):
    
    @run_on_arbiter
    def testSpawning(self):
        arbiter = pulsar.get_actor()
        self.assertEqual(arbiter.aid, 'arbiter')
        self.assertEqual(len(arbiter.monitors), 1)
        self.assertEqual(arbiter.monitors['test']._spawning, {})
        
    def testArbiter(self):
        worker = pulsar.get_actor()
        self.assertRaises(pulsar.PulsarException, pulsar.arbiter)
        arbiter = worker.arbiter
        self.assertTrue(arbiter)
        
    @run_on_arbiter
    def testArbiterObject(self):
        '''Test the arbiter in its process domain'''
        arbiter = pulsar.arbiter()
        self.assertTrue(arbiter.is_arbiter())
        self.assertEqual(arbiter.impl, 'monitor')
        self.assertTrue(arbiter.monitors)
        self.assertEqual(arbiter.ioloop, arbiter.requestloop)
        self.assertFalse(arbiter.cpubound)
        

