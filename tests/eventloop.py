import time
from threading import current_thread

import pulsar
from pulsar.apps.test import unittest


class TestEventLoop(unittest.TestCase):
    
    def testIOloop(self):
        ioloop = pulsar.thread_ioloop()
        self.assertTrue(ioloop)
        self.assertNotEqual(ioloop.tid, current_thread().ident)
        
    def test_add_callback(self):
        ioloop = pulsar.thread_ioloop()
        d = pulsar.Deferred()
        ioloop.add_callback(lambda: d.callback(current_thread().ident))
        # we should be able to wait less than a second
        yield d
        self.assertEqual(d.result, ioloop.tid)
        
    def test_add_timeout(self):
        ioloop = pulsar.thread_ioloop()
        d = pulsar.Deferred()
        now = time.time()
        timeout1 = ioloop.add_timeout(now+20,
                            lambda: d.callback(current_thread().ident))
        timeout2 = ioloop.add_timeout(now+10,
                            lambda: d.callback(current_thread().ident))
        # lets wake the ioloop
        ioloop.wake()
        self.assertTrue(timeout1 in ioloop._timeouts)
        self.assertTrue(timeout2 in ioloop._timeouts)
        ioloop.remove_timeout(timeout1)
        ioloop.remove_timeout(timeout2)
        self.assertFalse(timeout1 in ioloop._timeouts)
        self.assertFalse(timeout2 in ioloop._timeouts)
        timeout1 = ioloop.add_timeout(now+0.1,
                            lambda: d.callback(current_thread().ident))
        ioloop.wake()
        time.sleep(0.2)
        self.assertTrue(d.called)
        self.assertEqual(d.result, ioloop.tid)
        self.assertFalse(timeout1 in ioloop._timeouts)
        
    def test_periodic(self):
        ioloop = pulsar.thread_ioloop()
        d = pulsar.Deferred()
        class p:
            def __init__(self):
                self.c = 0
            def __call__(self, periodic):
                self.c += 1
                if self.c == 2:
                    raise ValueError()
                elif self.c == 3:
                    periodic.stop()
                    d.callback(self.c)
        periodic = ioloop.add_periodic(p(), 1)
        yield d
        self.assertEqual(d.result, 3)
        self.assertFalse(periodic._running)
        
    def ___test_stop(self):
        ioloop = pulsar.thread_ioloop()
        tid = ioloop.tid
        self.assertTrue(ioloop.running())
        self.assertFalse(ioloop.start())
        d = pulsar.Deferred()
        ioloop.stop().add_callback(d.callback)
        time.sleep(0.1)
        self.assertTrue(d.called)
        self.assertTrue(ioloop.stopped)
        self.assertFalse(ioloop.running())
        num_loops = ioloop.num_loops
        self.assertTrue(num_loops)
        self.assertRaises(RuntimeError, ioloop.start, pulsar.get_actor())
        ioloop.start()
        # give the thread chance to start
        time.sleep(0.1)
        self.assertTrue(ioloop.running())
        self.assertTrue(ioloop.num_loops < num_loops)
        self.assertNotEqual(tid, ioloop.tid)
        
        
        
        