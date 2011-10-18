'''Tests the test suite and pulsar distribution.'''
import os
import unittest as test
import threading
import multiprocessing

import pulsar


class TestTest(test.TestCase):
    
    def testWorker(self):
        worker = self.worker
        self.assertTrue(worker.running())
        self.assertEqual(worker.tid,threading.current_thread().ident)
        if worker.isprocess():
            self.assertEqual(worker.pid,os.getpid())
        

class TestPulsar(test.TestCase):
    
    def test_version(self):
        self.assertTrue(pulsar.VERSION)
        self.assertTrue(pulsar.__version__)
        self.assertEqual(pulsar.__version__,pulsar.get_version())
        self.assertTrue(len(pulsar.VERSION) >= 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__", "__doc__"):
            self.assertTrue(getattr(pulsar, m, None))