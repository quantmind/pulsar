import time
import unittest

from pulsar.apps import tasks

from .test_pulsards import TaskQueueBase


# class TestTaskQueueRestart(TaskQueueBase, unittest.TestCase):
# TODO: fix this test

class a:    # pragma    nocover
    def info(self):
        info = yield self.proxy.server_info()
        # get the task queue
        yield info['monitors'][self.name()]

    def test_kill_task_workers(self):
        tq = yield self.info()
        killed = set()
        for worker in tq['workers']:
            a = worker['actor']
            aid = a['actor_id']
            self.assertEqual(a['is_process'], self.concurrency == 'process')
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

    def __test_check_worker(self):
        # TODO, this test fails sometimes
        r = yield self.proxy.run_new_task(jobname='checkworker')
        self.assertTrue(r)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        result = r['result']
        concurrent = result['tasks']
        self.assertTrue(r['id'] in concurrent)
