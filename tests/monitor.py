'''Tests for arbiter and monitors.'''
import os
from threading import current_thread

import pulsar
from pulsar import send, spawn
from pulsar.apps.test import unittest, run_on_arbiter


class TestMonitor(unittest.TestCase):
    concurrency = 'thread'
    def get_monitor(self, name, num_workers=1, **kwargs):
        actor = actor or pulsar.Actor
        m = self.arbiter.add_monitor(pulsar.Monitor, name,
                                     num_workers = num_workers,
                                     actor_params = {'concurrency':impl})
        self.assertEqual(m.ioloop, self.arbiter.ioloop)
        self.assertTrue(m.is_alive())
        self.assertEqual(m.name,name)
        self.assertTrue(isinstance(m,Monitor))
        self.assertEqual(m.worker_class,actor)
        return m
    
    def stop_monitor(self, m):
        arbiter = self.arbiter
        self.assertEqual(len(m.LIVE_ACTORS),2)
        m.stop()
        self.wait(lambda : m.name in arbiter.monitors)
        self.assertFalse(m.name in arbiter.monitors)
        self.assertFalse(m.is_alive())
        
    def _testcreatemonitor(self, impl):
        '''Create a monitor with two actors and ping it from the arbiter.'''
        m = self.get_monitor(impl, 'testmonitor')
        self.sleep(1)
        cbk = self.Callback()
        self.arbiter.proxy.ping(m.proxy).add_callback(cbk)
        self.wait(lambda : not hasattr(cbk,'result'))
        self.assertEqual(cbk.result,"pong")
        self.stop_monitor(m)
        self.sleep(1)
        

