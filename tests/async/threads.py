from pulsar import ThreadPool, async_while, get_request_loop
from pulsar.utils.pep import get_event_loop
from pulsar.apps.test import unittest


class TestThreadQueue(unittest.TestCase):
    pool = None
    
    def get_pool(self, *args, **kwargs):
        if self.pool:
            raise RuntimeError('Only one pool per test please')
        self.pool = ThreadPool(*args, **kwargs)
        return self.pool
    
    def tearDown(self):
        if self.pool:
            return self.pool.terminate()
            
    def test_pool(self):
        pool = self.get_pool(threads=2)
        self.assertEqual(pool._state, 0)
        self.assertEqual(pool.status, 'running')
        self.assertEqual(pool.event_loop, get_event_loop())
        self.assertNotEqual(pool.event_loop, get_request_loop())
        #give a chance to start the pool
        yield async_while(3, lambda: not pool.num_threads)
        self.assertEqual(pool.num_threads, 2)
        pool.close()
        self.assertEqual(pool._state, 1)
        self.assertEqual(pool.status, 'closed')
        pool.join()
        yield async_while(3, lambda: pool.num_threads)
        self.assertFalse(pool.num_threads)
        
        