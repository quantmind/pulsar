'''MultiFuture coverage'''
import unittest
import asyncio

from pulsar import multi_async, Future


class TestMulti(unittest.TestCase):

    def test_empy_list(self):
        r = multi_async(())
        self.assertTrue(r.done())
        self.assertEqual(r.result(), [])

    def test_empy_dict(self):
        r = multi_async({})
        self.assertTrue(r.done())
        self.assertEqual(r.result(), {})

    @asyncio.coroutine
    def test_multi(self):
        d1 = Future()
        d2 = Future()
        d = multi_async([d1, d2, 'bla'])
        self.assertFalse(d.done())
        d2.set_result('first')
        self.assertFalse(d.done())
        d1.set_result('second')
        result = yield from d
        self.assertEqual(result, ['second', 'first', 'bla'])
