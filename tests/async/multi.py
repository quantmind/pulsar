'''MultiFuture coverage'''
import sys
import unittest

from pulsar import (get_event_loop, multi_async, InvalidStateError, Future,
                    maybe_async)


class TestApi(unittest.TestCase):

    def test_empy_list(self):
        r = multi_async(())
        self.assertTrue(r.done())
        self.assertEqual(r.result(), [])

    def test_empy_dict(self):
        r = multi_async({})
        self.assertTrue(r.done())
        self.assertEqual(r.result(), {})

    def test_simple_multi(self):
        d = multi_async(lock=False)
        self.assertFalse(d.done())
        self.assertFalse(d._locked)
        self.assertFalse(d._futures)
        self.assertFalse(d._stream)
        d.lock()
        self.assertTrue(d.done())
        self.assertTrue(d._locked)
        self.assertEqual(d.result(), [])
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(InvalidStateError, d._finish)

    def test_multi(self):
        d = multi_async(lock=False)
        d1 = Future()
        d2 = Future()
        d.append(d1)
        d.append(d2)
        d.append('bla')
        self.assertRaises(RuntimeError, d._finish)
        d.lock()
        self.assertRaises(RuntimeError, d._finish)
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(RuntimeError, d.append, d1)
        self.assertFalse(d.done())
        d2.callback('first')
        self.assertFalse(d.done())
        d1.callback('second')
        self.assertTrue(d.done())
        self.assertEqual(d.result(), ['second', 'first', 'bla'])

    def test_multi_update(self):
        d1 = Future()
        d2 = Future()
        d = multi_async(lock=False)
        d.update((d1, d2)).lock()
        d1.callback('first')
        d2.callback('second')
        self.assertTrue(d.done())
        self.assertEqual(d.result(), ['first', 'second'])

    def test_multi_nested(self):
        d = multi_async([a for a in range(1, 11)])
        r = maybe_async(d)
        self.assertNotIsInstance(r, Future)
        self.assertEqual(r, [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
