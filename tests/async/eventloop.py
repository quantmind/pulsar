import unittest
from threading import current_thread

import pulsar
from pulsar.async.eventloop import LoopingCall
from pulsar import (asyncio, run_in_loop, Future, call_repeatedly,
                    get_event_loop, new_event_loop, loop_thread_id)


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
        tid = yield loop_thread_id(ioloop)
        self.assertNotEqual(tid, current_thread().ident)

    def test_call_soon(self):
        ioloop = get_event_loop()
        tid = yield loop_thread_id(ioloop)
        d = Future()
        callback = lambda: d.set_result(current_thread().ident)
        cbk = ioloop.call_soon(callback)
        self.assertEqual(cbk._callback, callback)
        self.assertEqual(cbk._args, ())
        # we should be able to wait less than a second
        result = yield d
        self.assertEqual(result, tid)

    def test_call_later(self):
        ioloop = get_event_loop()
        tid = yield loop_thread_id(ioloop)
        d = Future()
        timeout1 = ioloop.call_later(
            20, lambda: d.set_result(current_thread().ident))
        timeout2 = ioloop.call_later(
            10, lambda: d.set_result(current_thread().ident))
        # lets wake the ioloop
        self.assertTrue(has_callback(ioloop, timeout1))
        self.assertTrue(has_callback(ioloop, timeout2))
        timeout1.cancel()
        timeout2.cancel()
        self.assertTrue(timeout1._cancelled)
        self.assertTrue(timeout2._cancelled)
        timeout1 = ioloop.call_later(
            0.1, lambda: d.set_result(current_thread().ident))
        yield d
        self.assertTrue(d.done())
        self.assertEqual(d.result(), tid)
        self.assertFalse(has_callback(ioloop, timeout1))

    def test_call_at(self):
        loop = get_event_loop()
        d1 = Future()
        d2 = Future()
        c1 = loop.call_at(loop.time()+1, lambda: d1.set_result(loop.time()))
        c2 = loop.call_later(1, lambda: d2.set_result(loop.time()))
        t1, t2 = yield pulsar.multi_async((d1, d2))
        self.assertTrue(t1 <= t2)

    def test_periodic(self):
        test = self
        loop = get_event_loop()
        waiter = Future()

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
                        waiter.set_result(self.c)
                        raise

        every = 2
        loops = 2
        track = p(loops)
        start = loop.time()
        periodic = call_repeatedly(loop, every, track)
        self.assertIsInstance(periodic, LoopingCall)
        done = yield waiter
        taken = loop.time() - start
        self.assertEqual(done, loops)
        self.assertTrue(taken > every*loops)
        self.assertTrue(taken < every*loops + 2)
        self.assertTrue(periodic.cancelled)
        self.assertFalse(has_callback(loop, periodic.handler))

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
