'''Tests the test suite and pulsar distribution.'''
import os
import time
from threading import current_thread

import pulsar
from pulsar import defaults, send, is_async, multi_async, is_async, is_failure
from pulsar.apps.test import unittest, run_on_arbiter, TestSuite
from pulsar.utils.pep import get_event_loop

def simple_function(actor):
    return 'success'

def wait(actor, period=0.5):
    time.sleep(period)
    return period


class TestTestWorker(unittest.TestCase):
    
    def testWorker(self):
        '''Test the test worker'''
        worker = pulsar.get_actor()
        self.assertTrue(pulsar.is_actor(worker))
        self.assertTrue(worker.running())
        self.assertFalse(worker.closed())
        self.assertFalse(worker.stopped())
        self.assertEqual(worker.info_state, 'running')
        self.assertEqual(worker.tid, current_thread().ident)
        self.assertEqual(worker.pid, os.getpid())
        self.assertTrue(worker.impl.daemon)
        self.assertFalse(worker.is_monitor())
        
    def testCPUbound(self):
        worker = pulsar.get_actor()
        self.assertTrue(worker.cpubound)
        self.assertTrue(worker.requestloop.cpubound)
        self.assertFalse(worker.ioloop.cpubound)
        self.assertEqual(worker.ioloop, worker.mailbox.event_loop)
        
    def testWorkerMonitor(self):
        worker = pulsar.get_actor()
        mailbox = worker.mailbox
        monitor = worker.monitor
        self.assertEqual(mailbox.address, monitor.address)
        self.assertEqual(mailbox.timeout, 0)
        
    @run_on_arbiter
    def test_TestSuiteMonitor(self):
        arbiter = pulsar.get_actor()
        self.assertTrue(len(arbiter.monitors) >= 1)
        monitor = arbiter.registered['test']
        app = monitor.app
        self.assertTrue(isinstance(app, TestSuite))
        self.assertFalse(monitor.cpubound)
        
    def test_Mailbox(self):
        worker = pulsar.get_actor()
        mailbox = worker.mailbox
        self.assertTrue(mailbox)
        self.assertTrue(mailbox.event_loop)
        self.assertTrue(mailbox.event_loop.running)
        self.assertNotEqual(worker.requestloop, mailbox.event_loop)
        self.assertNotEqual(worker.tid, mailbox.event_loop.tid)
        self.assertTrue(mailbox.address)
        self.assertTrue(mailbox.name)
        self.assertEqual(mailbox.max_connections, 1)
        
    def testIOloop(self):
        '''Test event loop in test worker'''
        worker = pulsar.get_actor()
        ioloop = get_event_loop()
        self.assertTrue(ioloop.running)
        self.assertNotEqual(worker.requestloop, ioloop)
        self.assertEqual(worker.ioloop, ioloop)
        self.assertEqual(worker.tid, worker.requestloop.tid)
        self.assertNotEqual(worker.tid, ioloop.tid)
        self.assertTrue(str(ioloop))
        
    def test_NOT_DONE(self):
        worker = pulsar.get_actor()
        count = worker.requestloop.num_loops
        yield pulsar.NOT_DONE
        self.assertFalse(worker.requestloop._callbacks)
        self.assertEqual(worker.requestloop.num_loops, count+1)
        yield pulsar.NOT_DONE
        self.assertEqual(worker.requestloop.num_loops, count+2)
        
    def testYield(self):
        '''Yielding a deferred calling back on separate thread'''
        worker = pulsar.get_actor()
        self.assertEqual(worker.tid, current_thread().ident)
        yield pulsar.NOT_DONE
        self.assertEqual(worker.tid, current_thread().ident)
        d = pulsar.Deferred()
        # We are calling back the deferred in the ioloop which is on a separate
        # thread
        def _callback():
            d.callback(current_thread().ident)
        worker.ioloop.call_later(0.2, _callback)
        yield d
        self.assertNotEqual(d.result, worker.tid)
        self.assertEqual(worker.ioloop.tid, d.result)
        self.assertEqual(worker.tid, current_thread().ident)
    
    def __testReconnect(self):
        #TODO: new test for drop connections
        worker = pulsar.get_actor()
        # the worker closes all arbiters connection
        worker.arbiter.mailbox.close_connections()
        response = send(worker.arbiter, 'ping')
        yield self.async.assertEqual(response.when_ready, 'pong')
        
    def testPingMonitor(self):
        worker = pulsar.get_actor()
        future = send('monitor', 'ping')
        self.assertTrue(is_async(future))
        yield future
        self.assertEqual(future.result, 'pong')
        yield self.async.assertEqual(send(worker.monitor, 'ping'), 'pong')
        response = worker.send(worker.monitor, 'notify', worker.info())
        yield response
        self.assertTrue(response.result < time.time())
        
    def __test_run_on_arbiter(self):
        actor = pulsar.get_actor()
        response = actor.send('arbiter', 'run', simple_function)
        yield response.when_ready
        self.assertEqual(response.result, 'success')
        
    def test_unknown_send_target(self):
        # The target does not exists
        future = pulsar.send('vcghdvchdgcvshcd', 'ping').add_both(lambda r: [r])
        yield future
        self.assertTrue(is_failure(future.result[0]))
        
    def test_multiple_execute(self):
        result1 = send('arbiter', 'run', wait, 0.2)
        result2 = send('arbiter', 'ping')
        result3 = send('arbiter', 'echo', 'ciao!')
        result4 = send('arbiter', 'run', wait, 0.1)
        result5 = send('arbiter', 'echo', 'ciao again!')
        yield multi_async((result1, result2, result3, result4, result5))
        self.assertEqual(result1.result, 0.2)
        self.assertEqual(result2.result, 'pong')
        self.assertEqual(result3.result, 'ciao!')
        self.assertEqual(result4.result, 0.1)
        self.assertEqual(result5.result, 'ciao again!')
        
        
class TestPulsar(unittest.TestCase):
    
    def test_version(self):
        self.assertTrue(pulsar.VERSION)
        self.assertTrue(pulsar.__version__)
        self.assertEqual(pulsar.__version__,pulsar.get_version(pulsar.VERSION))
        self.assertTrue(len(pulsar.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(pulsar, m, None))