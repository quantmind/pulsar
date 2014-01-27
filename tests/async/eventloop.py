import time
import sys
import asyncio
import unittest
from threading import current_thread

import pulsar
from pulsar import (run_in_loop, Future, TimeoutError,
                    get_event_loop, new_event_loop)


def has_callback(loop, handler):
    if isinstance(handler, asyncio.TimerHandle):
        return handler in loop._scheduled
    else:
        return handler in loop._ready


class TestEventLoop(unittest.TestCase):

    def test_request_loop(self):
        request_loop = pulsar.get_request_loop()
        event_loop = get_event_loop()
        self.assertNotEqual(event_loop, request_loop)

    def test_io_loop(self):
        ioloop = get_event_loop()
        self.assertTrue(ioloop)
        self.assertNotEqual(ioloop.tid, current_thread().ident)

    def test_call_soon(self):
        ioloop = get_event_loop()
        d = Future()
        callback = lambda: d.callback(current_thread().ident)
        cbk = ioloop.call_soon(callback)
        self.assertEqual(cbk._callback, callback)
        self.assertEqual(cbk._args, ())
        # we should be able to wait less than a second
        result = yield d
        self.assertEqual(result, ioloop.tid)

    def test_call_later(self):
        ioloop = get_event_loop()
        d = Future()
        timeout1 = ioloop.call_later(
            20, lambda: d.callback(current_thread().ident))
        timeout2 = ioloop.call_later(
            10, lambda: d.callback(current_thread().ident))
        # lets wake the ioloop
        self.assertTrue(has_callback(ioloop, timeout1))
        self.assertTrue(has_callback(ioloop, timeout2))
        timeout1.cancel()
        timeout2.cancel()
        self.assertTrue(timeout1._cancelled)
        self.assertTrue(timeout2._cancelled)
        timeout1 = ioloop.call_later(
            0.1, lambda: d.callback(current_thread().ident))
        yield d
        self.assertTrue(d.done())
        self.assertEqual(d.result(), ioloop.tid)
        self.assertFalse(has_callback(ioloop, timeout1))

    def test_call_later_cheat(self):
        ioloop = get_event_loop()

        def dummy(d, sleep):
            d.callback(time.time())
            time.sleep(sleep)

        d1 = Future()
        d2 = Future()
        ioloop.call_later(0, dummy, d1, 0.1)
        ioloop.call_later(-5, dummy, d2, 0.1)
        yield d2
        self.assertTrue(d2.result() < d1.result())

    def test_call_at(self):
        ioloop = get_event_loop()
        d1 = Future()
        d2 = Future()
        c1 = ioloop.call_at(ioloop.time()+1,
                            lambda: d1.callback(ioloop.time()))
        c2 = ioloop.call_later(1, lambda: d2.callback(ioloop.time()))
        t1, t2 = yield pulsar.multi_async((d1, d2))
        self.assertTrue(t1 <= t2)

    def test_periodic(self):
        test = self
        ioloop = get_event_loop()
        d = Future()

        class p:
            def __init__(self, loops):
                self.loops = loops
                self.c = 0

            def __call__(self):
                self.c += 1
                if self.c == self.loops:
                    try:
                        raise ValueError('test periodic')
                    except Exception:
                        mute_failure(test, sys.exc_info())
                        raise
                    finally:
                        d.callback(self.c)

        every = 2
        loops = 2
        track = p(loops)
        start = time.time()
        periodic = ioloop.call_repeatedly(every, track)
        loop = yield d
        taken = time.time() - start
        self.assertEqual(loop, loops)
        self.assertTrue(taken > every*loops)
        self.assertTrue(taken < every*loops + 2)
        self.assertTrue(periodic.cancelled)
        self.assertFalse(has_callback(ioloop, periodic.handler))

    def test_call_every(self):
        test = self
        ioloop = get_event_loop()
        thread = current_thread()
        d = Future()
        test = self

        class p:
            def __init__(self, loop):
                self.loop = loop
                self.c = 0
                self.prev_loop = 0

            def __call__(self):
                try:
                    test.assertNotEqual(current_thread(), thread)
                    if self.prev_loop:
                        test.assertEqual(ioloop.num_loops, self.prev_loop+1)
                except Exception:
                    d.callback(sys.exc_info())
                else:
                    self.prev_loop = ioloop.num_loops
                    self.c += 1
                    if self.c == self.loop:
                        d.callback(self.c)
                        try:
                            raise ValueError('test call every')
                        except Exception:
                            mute_failure(test, Failure(sys.exc_info()))
                            raise
        #
        loops = 5
        track = p(loops)
        start = time.time()
        periodic = ioloop.call_every(track)
        loop = yield d
        self.assertEqual(loop, loops)
        self.assertTrue(periodic.cancelled)
        self.assertFalse(has_callback(ioloop, periodic.handler))

    def test_run_until_complete(self):
        event_loop = new_event_loop()
        self.assertFalse(event_loop.running)
        self.assertFalse(event_loop.iothreadloop)
        self.assertEqual(str(event_loop), '<not running> pulsar')
        d = Future()
        event_loop.call_later(2, d.callback, 'OK')
        event_loop.run_until_complete(d)
        self.assertTrue(d.done())
        self.assertEqual(d.result(), 'OK')
        self.assertFalse(event_loop.running)

    def test_run_until_complete_timeout(self):
        event_loop = new_event_loop()
        self.assertFalse(event_loop.is_running())
        self.assertFalse(event_loop.iothreadloop)
        d = Future().set_timeout(2)
        event_loop.call_later(10, d.callback, 'OK')
        self.assertRaises(TimeoutError, event_loop.run_until_complete, d)
        self.assertTrue(d.done())
        d.exception()  # mute failure
        self.assertFalse(event_loop.is_running())

    def test_run_in_thread_loop(self):
        event_loop = get_event_loop()

        def simple(a, b):
            return a + b

        d = run_in_loop(event_loop, simple, 1, 2)
        self.assertIsInstance(d, Future)
        result = yield d
        self.assertEqual(result, 3)
        d = run_in_loop(event_loop, simple, 1, 'a')
        self.assertIsInstance(d, Future)
        try:
            result = yield d
        except TypeError:
            pass
        else:
            assert False, "TypeError not raised"
