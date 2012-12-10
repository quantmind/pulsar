'''Tests the test suite and pulsar distribution.'''
import os
import time
import pickle
from threading import current_thread

import pulsar
from pulsar import defaults, send, is_async
from pulsar.apps.test import unittest, run_on_arbiter, TestSuite

def simple_function(actor):
    return 'success'

class wait:
    
    def __init__(self, period=0.5):
        self.period = period
        
    def __call__(self, actor):
        time.sleep(self.period)
        return self.period


class TestTestWorker(unittest.TestCase):
    
    def testWorker(self):
        worker = pulsar.get_actor()
        self.assertTrue(pulsar.is_actor(worker))
        self.assertTrue(worker.running())
        self.assertTrue(worker.ready())
        self.assertFalse(worker.stopping())
        self.assertFalse(worker.closed())
        self.assertFalse(worker.stopped())
        self.assertEqual(worker.info_state, 'running')
        self.assertEqual(worker.tid, current_thread().ident)
        self.assertEqual(worker.pid, os.getpid())
        self.assertTrue(worker.cpubound)
        self.assertTrue(worker.mailbox.cpubound)
        self.assertTrue(worker.impl.daemon)
        self.assertFalse(worker.is_pool())
        
    def testWorkerMonitor(self):
        worker = pulsar.get_actor()
        monitor = worker.monitor
        arbiter = worker.arbiter
        mailbox = monitor.mailbox
        self.assertEqual(mailbox, arbiter.mailbox)
        self.assertTrue(mailbox.async)
        
    @run_on_arbiter
    def testTestSuiteMonitor(self):
        arbiter = pulsar.get_actor()
        self.assertEqual(len(arbiter.monitors), 1)
        monitor = list(arbiter.monitors.values())[0]
        app = monitor.app
        self.assertTrue(isinstance(app, TestSuite))
        self.assertFalse(monitor.cpubound)
        self.assertFalse(monitor.mailbox.cpubound)
        
    def testMailbox(self):
        worker = pulsar.get_actor()
        mailbox = worker.mailbox
        self.assertTrue(mailbox)
        self.assertTrue(mailbox.ioloop)
        self.assertTrue(mailbox.ioloop.running())
        self.assertNotEqual(worker.requestloop, mailbox.ioloop)
        self.assertNotEqual(worker.tid, mailbox.ioloop.tid)
        self.assertTrue(mailbox.address)
        self.assertTrue(mailbox.sock)
        
    def testIOloop(self):
        worker = pulsar.get_actor()
        ioloop = pulsar.thread_ioloop()
        self.assertTrue(ioloop.running())
        self.assertNotEqual(worker.requestloop, ioloop)
        self.assertEqual(worker.ioloop, ioloop)
        self.assertEqual(worker.tid, worker.requestloop.tid)
        self.assertNotEqual(worker.tid, ioloop.tid)
        self.assertTrue(str(ioloop))
        self.assertFalse(ioloop.start())
        
    def testNOT_DONE(self):
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
        worker.ioloop.add_timeout(time.time()+0.2, _callback)
        yield d
        self.assertNotEqual(d.result, worker.tid)
        self.assertEqual(worker.ioloop.tid, d.result)
        self.assertEqual(worker.tid, current_thread().ident)
    
    def testReconnect(self):
        worker = pulsar.get_actor()
        # the worker shut down the arbiter mailbox
        c = worker.arbiter.mailbox
        c.close()
        yield self.async.assertEqual(c.ping(), 'pong')
        
    def testPingArbiter(self):
        worker = pulsar.get_actor()
        yield self.async.assertEqual(send('arbiter', 'ping'), 'pong')
        yield self.async.assertEqual(send('arbiter', 'ping'), 'pong')
        result = worker.send('arbiter', 'notify', worker.info())
        if is_async(result):
            yield outcome
            result = outcome.result
        self.assertTrue(result>0)
        yield self.async.assertEqual(send('arbiter', 'ping'), 'pong')
        yield self.async.assertEqual(send('arbiter', 'echo', 'ciao'), 'ciao')
        
    def test_run_on_arbiter(self):
        actor = pulsar.get_actor()
        result = actor.send('arbiter', 'run', simple_function)
        yield result
        self.assertEqual(result.result, 'success')
        
    def test_bad_send(self):
        # The target does not exists
        self.assertRaises(ValueError, pulsar.send, 'vcghdvchdgcvshcd', 'ping')
        
    def test_multiple_execute(self):
        result1 = pulsar.send('arbiter', 'run', wait(0.2))
        result2 = pulsar.send('arbiter', 'ping')
        result3 = pulsar.send('arbiter', 'echo', 'ciao!')
        result4 = pulsar.send('arbiter', 'run', wait(0.1))
        result5 = pulsar.send('arbiter', 'echo', 'ciao again!')
        yield result5
        self.assertEqual(result1.result, 0.2)
        self.assertEqual(result2.result, 'pong')
        self.assertEqual(result3.result, 'ciao!')
        self.assertEqual(result4.result, 0.1)
        self.assertEqual(result5.result, 'ciao again!')
        
    #def testPickle(self):
    #    self.assertRaises(pickle.PicklingError, pickle.dumps,
    #                      pulsar.get_actor())

class TestPulsar(unittest.TestCase):
    
    def test_version(self):
        self.assertTrue(pulsar.VERSION)
        self.assertTrue(pulsar.__version__)
        self.assertEqual(pulsar.__version__,pulsar.get_version(pulsar.VERSION))
        self.assertTrue(len(pulsar.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(pulsar, m, None))