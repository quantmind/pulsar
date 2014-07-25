'''MultiFuture coverage'''
import sys
import unittest

from pulsar import (get_event_loop, multi_async, InvalidStateError, Future,
                    maybe_async)


class TestMulti(unittest.TestCase):

    def test_empy_list(self):
        r = multi_async(())
        self.assertTrue(r.done())
        self.assertEqual(r.result(), [])

    def test_empy_dict(self):
        r = multi_async({})
        self.assertTrue(r.done())
        self.assertEqual(r.result(), {})

    def test_multi(self):
        d1 = Future()
        d2 = Future()
        d = multi_async([d1, d2, 'bla'])
        self.assertFalse(d.done())
        d2.set_result('first')
        self.assertFalse(d.done())
        d1.set_result('second')
        result = yield d
        self.assertEqual(result, ['second', 'first', 'bla'])
