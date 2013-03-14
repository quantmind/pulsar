import time
from threading import current_thread

import pulsar
from pulsar.utils.pep import get_event_loop
from pulsar.apps.test import unittest


class TestEventLoop(unittest.TestCase):
    
    def testIOloop(self):
        ioloop = get_event_loop()
        self.assertTrue(ioloop)
        self.assertNotEqual(ioloop.tid, current_thread().ident)
        
    def test_add_callback(self):
        ioloop = get_event_loop()
        d = pulsar.Deferred()
        callback = lambda: d.callback(current_thread().ident)
        cbk = ioloop.call_soon(callback)
        self.assertEqual(cbk.callback, callback)
        self.assertEqual(cbk.args, ())
        # we should be able to wait less than a second
        yield d
        self.assertEqual(d.result, ioloop.tid)
        
    def test_add_timeouts(self):
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
        self.assertTrue(d.called)
        self.assertEqual(d.result, ioloop.tid)
        self.assertFalse(ioloop.has_callback(timeout1))
        
    def test_periodic(self):
        ioloop = get_event_loop()
        class p:
            def __init__(self):
                self.c = 0
            def __call__(self):
                self.c += 1
                if self.c == 2:
                    raise ValueError()
        track = p()
        periodic = ioloop.call_repeatedly(1, track)
        while track.c < 2:
            yield pulsar.NOT_DONE
        self.assertEqual(track.c, 2)
        self.assertFalse(ioloop.has_callback(periodic))
        
    def test_call_every(self):
        ioloop = get_event_loop()
        class p:
            def __init__(self):
                self.c = 0
            def __call__(self):
                self.c += 1
                if self.c == 2:
                    raise ValueError()
        track = p()
        periodic = ioloop.call_every(track)
        while track.c < 2:
            yield pulsar.NOT_DONE
        self.assertEqual(track.c, 2)
        self.assertFalse(ioloop.has_callback(periodic))
        
        
        
        