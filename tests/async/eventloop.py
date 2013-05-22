import time
from threading import current_thread

import pulsar
from pulsar.utils.pep import get_event_loop, new_event_loop
from pulsar.apps.test import unittest


class TestEventLoop(unittest.TestCase):
    
    def testIOloop(self):
        ioloop = get_event_loop()
        self.assertTrue(ioloop)
        self.assertNotEqual(ioloop.tid, current_thread().ident)
        
    def test_call_soon(self):
        ioloop = get_event_loop()
        d = pulsar.Deferred()
        callback = lambda: d.callback(current_thread().ident)
        cbk = ioloop.call_soon(callback)
        self.assertEqual(cbk.callback, callback)
        self.assertEqual(cbk.args, ())
        # we should be able to wait less than a second
        yield d
        self.assertEqual(d.result, ioloop.tid)
        
    def test_call_later(self):
        ioloop = get_event_loop()
        d = pulsar.Deferred()
        timeout1 = ioloop.call_later(20,
                            lambda: d.callback(current_thread().ident))
        timeout2 = ioloop.call_later(10,
                            lambda: d.callback(current_thread().ident))
        # lets wake the ioloop
        self.assertTrue(ioloop.has_callback(timeout1))
        self.assertTrue(ioloop.has_callback(timeout2))
        timeout1.cancel()
        timeout2.cancel()
        self.assertTrue(timeout1.cancelled)
        self.assertTrue(timeout2.cancelled)
        timeout1 = ioloop.call_later(0.1,
                            lambda: d.callback(current_thread().ident))
        yield d
        self.assertTrue(d.done())
        self.assertEqual(d.result, ioloop.tid)
        self.assertFalse(ioloop.has_callback(timeout1))
        
    def test_call_later_cheat(self):
        ioloop = get_event_loop()
        def dummy(d, sleep=None):
            d.callback(time.time())
            if sleep:
                time.sleep(sleep)
        d1 = pulsar.Deferred()
        d2 = pulsar.Deferred()
        ioloop.call_later(0, dummy, d1, 0.2)
        ioloop.call_later(-5, dummy, d2)
        yield d2
        self.assertTrue(d1.result < d2.result)
        
    def test_call_at(self):
        ioloop = get_event_loop()
        d1 = pulsar.Deferred()
        d2 = pulsar.Deferred()
        c1 = ioloop.call_at(ioloop.timer()+1, lambda: d1.callback(ioloop.timer()))
        c2 = ioloop.call_later(1, lambda: d2.callback(ioloop.timer()))
        t1, t2 = yield pulsar.multi_async((d1, d2))
        self.assertTrue(t1 < t2)
        
    def test_periodic(self):
        ioloop = get_event_loop()
        d = pulsar.Deferred()
        class p:
            def __init__(self, loop):
                self.loop = loop
                self.c = 0
            def __call__(self):
                self.c += 1
                print(self.c)
                if self.c == self.loop:
                    d.callback(self.c)
                    raise ValueError()
        track = p(2)
        start = time.time()
        periodic = ioloop.call_repeatedly(1, track)
        loop = yield d
        taken = time.time() - start
        print(taken)
        self.assertEqual(loop, 2)
        self.assertFalse(ioloop.has_callback(periodic))
        
    def test_call_every(self):
        ioloop = get_event_loop()
        d = pulsar.Deferred()
        class p:
            def __init__(self, loop):
                self.loop = loop
                self.c = 0
            def __call__(self):
                self.c += 1
                if self.c == self.loop:
                    d.callback(self.c)
                    raise ValueError()
        track = p(2)
        periodic = ioloop.call_every(track)
        loop = yield d
        self.assertEqual(loop, 2)
        self.assertFalse(ioloop.has_callback(periodic))
        
    def test_run_until_complete(self):
        event_loop = new_event_loop(iothreadloop=False)
        self.assertFalse(event_loop.running)
        self.assertFalse(event_loop.iothreadloop)
        self.assertEqual(str(event_loop), 'EventLoop <not running>')
        d = pulsar.Deferred()
        event_loop.call_later(2, d.callback, 'OK')
        event_loop.run_until_complete(d)
        self.assertTrue(d.done())
        self.assertEqual(d.result, 'OK')
        self.assertFalse(event_loop.running)
        
    def test_run_until_complete_timeout(self):
        event_loop = new_event_loop(iothreadloop=False)
        self.assertFalse(event_loop.running)
        self.assertFalse(event_loop.iothreadloop)
        d = pulsar.Deferred()
        event_loop.call_later(10, d.callback, 'OK')
        self.assertRaises(pulsar.TimeoutError, 
                          event_loop.run_until_complete, d, timeout=2)
        self.assertFalse(d.done())
        self.assertFalse(event_loop.running)
        
        