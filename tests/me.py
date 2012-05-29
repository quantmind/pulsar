'''Tests the test suite and pulsar distribution.'''
import os
import time
from threading import current_thread
import multiprocessing

import pulsar
from pulsar import defaults
from pulsar.apps.test import unittest, run_on_arbiter

class TestTestWorker(unittest.TestCase):
    
    def testWorker(self):
        worker = pulsar.get_actor()
        self.assertTrue(pulsar.is_actor(worker))
        self.assertTrue(worker.running())
        self.assertFalse(worker.closed())
        self.assertFalse(worker.stopped())
        self.assertEqual(worker.state, 'running')
        self.assertEqual(worker.tid, current_thread().ident)
        self.assertEqual(worker.pid, os.getpid())
        self.assertTrue(worker.cpubound)
        
    def testWorkerMonitor(self):
        worker = pulsar.get_actor()
        monitor = worker.monitor
        arbiter = worker.arbiter
        mailbox = monitor.mailbox
        self.assertEqual(mailbox, arbiter.mailbox)
        self.assertTrue(mailbox.async)
        
    def testMailbox(self):
        worker = pulsar.get_actor()
        mailbox = worker.mailbox
        self.assertTrue(mailbox)
        self.assertTrue(mailbox.ioloop)
        self.assertTrue(mailbox.ioloop.running())
        self.assertNotEqual(worker.requestloop, mailbox.ioloop)
        self.assertNotEqual(worker.tid, mailbox.ioloop.tid)
        self.assertTrue(mailbox.address)
        self.assertTrue(mailbox.socket)
        
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
        c = worker.arbiter.mailbox
        c.close()
        yield self.async.assertEqual(c.ping(), 'pong')
        
    def testPingArbiter(self):
        worker = pulsar.get_actor()
        c = worker.arbiter.mailbox
        yield self.async.assertEqual(c.ping(), 'pong')
        yield self.async.assertEqual(c.ping(), 'pong')
        self.assertTrue(worker.send(worker.arbiter, 'notify', worker.info())>0)
        yield self.async.assertEqual(c.ping(), 'pong')
        c = worker.monitor.mailbox
        yield self.async.assertEqual(c.ping(), 'pong')
        yield self.async.assertEqual(c.echo('ciao'), 'ciao')


class TestPulsar(unittest.TestCase):
    
    def test_version(self):
        self.assertTrue(pulsar.VERSION)
        self.assertTrue(pulsar.__version__)
        self.assertEqual(pulsar.__version__,pulsar.get_version(pulsar.VERSION))
        self.assertTrue(len(pulsar.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(pulsar, m, None))