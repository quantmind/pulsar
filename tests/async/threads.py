from pulsar import ThreadPool
from pulsar.utils.pep import get_event_loop
from pulsar.apps.test import unittest


class TestThreadQueue(unittest.TestCase):
    
    def test_pool(self):
        pool = ThreadPool(processes=2)
        self.assertEqual(pool._state, 0)
        pool.close()
        self.assertEqual(pool._state, 1)
        pool.join()
        
        