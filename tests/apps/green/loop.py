from pulsar.apps.green import greenlet, GreenEventLoop, GreenTask
from pulsar.apps.test import unittest


@unittest.skipUnless(greenlet, 'Requires the greenlet package')
class TestGreenEventLoop(unittest.TestCase):
    
    def test_create(self):
        loop = GreenEventLoop(iothreadloop=False)
        self.assertEqual(loop.task_factory, GreenTask)
    
    