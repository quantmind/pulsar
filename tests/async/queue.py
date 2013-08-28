from pulsar import Queue, Empty, maybe_async, Deferred, Full, get_request_loop
from pulsar.utils.pep import default_timer, get_event_loop
from pulsar.apps.test import unittest


class TestQueue(unittest.TestCase):
    
    def test_sync(self):
        q = Queue()
        self.assertEqual(q.qsize(), 0)
        result = yield q.put('hello')
        self.assertEqual(result, None)
        self.assertEqual(q.qsize(), 1)
        item = yield q.get()
        self.assertEqual(item, 'hello')
        self.assertEqual(q.qsize(), 0)
        result = yield q.put('ciao')
        self.assertEqual(result, None)
        self.assertEqual(q.qsize(), 1)
        
    def test_timeout(self):
        q = Queue()
        self.assertEqual(q.qsize(), 0)
        start = default_timer()
        try:
            item = yield q.get(timeout=0.5)
        except Empty:
            pass
        self.assertTrue(default_timer()-start >= 0.5)
        self.assertEqual(q.qsize(), 0)
        
    def test_timeout_and_put(self):
        q = Queue()
        self.assertEqual(q.qsize(), 0)
        start = default_timer()
        try:
            item = yield q.get(timeout=0.5)
        except Empty:
            pass
        self.assertTrue(default_timer()-start >= 0.5)
        self.assertEqual(q.qsize(), 0)
        yield q.put('hello')
        self.assertEqual(q.qsize(), 1)
        item = yield q.get()
        self.assertEqual(item, 'hello')
        self.assertEqual(q.qsize(), 0)
        
    def test_async_get(self):
        q = Queue()
        self.assertEqual(q.qsize(), 0)
        item = maybe_async(q.get())
        self.assertIsInstance(item , Deferred)
        result = yield q.put('Hello')
        self.assertEqual(result, None)
        self.assertTrue(item.done())
        self.assertEqual(item.result, 'Hello')
        self.assertEqual(q.qsize(), 0)
        
    def test_maxsize(self):
        q = Queue(maxsize=2)
        self.assertEqual(q.maxsize, 2)
        yield self.async.assertEqual(q.put('hello'), None)
        yield self.async.assertEqual(q.put('ciao'), None)
        self.assertEqual(q.qsize(), 2)
        self.assertTrue(q.full())
        start = default_timer()
        yield self.async.assertRaises(Full, q.put, 'ciao', timeout=0.5)
        self.assertTrue(default_timer()-start >= 0.5)
        
    def test_maxsize_callback(self):
        q = Queue(maxsize=2)
        self.assertEqual(q.maxsize, 2)
        yield self.async.assertEqual(q.put('hello'), None)
        yield self.async.assertEqual(q.put('ciao'), None)
        result = maybe_async(q.put('bla'))
        self.assertEqual(q.qsize(), 2)
        item = yield q.get()
        self.assertEqual(item, 'hello')
        self.assertEqual(q.qsize(), 2)
        
    def test_event_loop(self):
        q1 = Queue(event_loop=get_request_loop())
        q2 = Queue(event_loop=get_event_loop())
        q3 = Queue()
        self.assertEqual(q2.event_loop, q3.event_loop)
        self.assertNotEqual(q1.event_loop, q3.event_loop)
        
    def test_put_timeout(self):
        q = Queue(maxsize=2)
        self.assertEqual(q.maxsize, 2)
        yield self.async.assertEqual(q.put('hello'), None)
        yield self.async.assertEqual(q.put('ciao'), None)
        yield self.async.assertRaises(Full, q.put, 'bla1', timeout=0.5)
        result = maybe_async(q.put('bla2'))
        self.assertEqual(q.qsize(), 2)
        item = yield q.get()
        self.assertEqual(item, 'hello')
        self.assertEqual(q.qsize(), 2)
        item = yield q.get()
        self.assertEqual(item, 'ciao')
        item = yield q.get()
        self.assertEqual(item, 'bla2')
        self.assertEqual(q.qsize(), 0)
        