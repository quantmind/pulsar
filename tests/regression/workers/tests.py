import os
from time import sleep

from pulsar import test, WorkerPool
from pulsar.workers import ttaskworker, ptaskworker


class App(object):
    
    def handler(self):
        return self
    

class TestThreadTaskWorker(test.TestCase):
    worker = ttaskworker.Worker
    num_workers = 2
    
    def pool(self, timeout = 30):
        self.p = WorkerPool(self.worker,
                            self.num_workers,
                            app = App(),
                            timeout = timeout)
        return self.p
        
    def testWorker(self):
        p = self.pool()
        self.assertEqual(p.worker_class,self.worker)
        self.assertFalse(p.is_alive())
        self.assertEqual(p.timeout,30)
        self.assertEqual(p.num_workers,self.num_workers)
        
    def testStartStop(self):
        p = self.pool()
        p.start()
        sleep(0.1)
        self.assertTrue(p.is_alive())
        self.assertEqual(len(p.WORKERS),self.num_workers)
        for w in p.WORKERS.values():
            worker = w['worker']
            self.assertTrue(worker.is_alive())
            self.assertTrue(worker.age)
        p.close()
        sleep(0.1)
        self.assertFalse(p.is_alive())
        
    

class TestProcessTaskWorker(TestThreadTaskWorker):
    worker = ptaskworker.Worker
