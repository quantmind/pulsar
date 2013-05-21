import time
from pulsar.apps.test import unittest

from .test_local import TaskQueueBase



class TestTaskQueueRestart(TaskQueueBase, unittest.TestCase):
    
    def test_kill_task_workers(self):
        info = yield self.proxy.server_info()
        # get the task queue
        tq = info['monitors'][self.name()]
        killed = set()
        for worker in tq['workers']:
            a = worker['actor']
            aid = a['actor_id']
            self.assertEqual(a['is_process'], self.concurrency=='process')
            r = yield self.proxy.kill_actor(aid)
            killed.add(aid)
            self.assertTrue(r)
        # lets get the info again
        start = time.time()
        workers = None 
        while time.time() - start < 5:
            info = yield self.proxy.server_info()
            tq = info['monitors'][self.name()]
            workers = tq['workers']
            if workers:
                break
        self.assertTrue(workers)
        for w in workers:
            a = w['actor']
            self.assertFalse(a['actor_id'] in killed)