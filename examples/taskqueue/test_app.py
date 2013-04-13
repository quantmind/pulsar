'''Tests the "taskqueue" example.'''
from time import time, sleep
from datetime import datetime, timedelta

from pulsar import send, get_application, get_actor, multi_async
from pulsar.apps import tasks, rpc
from pulsar.utils.timeutils import timedelta_seconds
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread,\
                                sequential

from .manage import server


CODE_TEST = '''\
import time
def task_function(N = 10, lag = 0.1):
    time.sleep(lag)
    return N*N
'''        
def wait_for_task(proxy, result):
    while result['status'] in tasks.UNREADY_STATES:
        result = yield proxy.get_task(id=result['id'])
    yield result
            

class TaskQueueBase(object):
    concurrency = 'thread'
    apps = ()
    
    @classmethod
    def setUpClass(cls):
        # The name of the task queue application
        s = server(name=cls.__name__, rpc_bind='127.0.0.1:0',
                   concurrency=cls.concurrency, 
                   rpc_concurrency=cls.concurrency,
                   script=__file__,
                   schedule_periodic=True)
        cls.apps = yield send('arbiter', 'run', s)
        cls.proxy = rpc.JsonProxy('http://%s:%s' % cls.apps[1].address)

    @classmethod
    def tearDownClass(cls):
        cmnds = [send('arbiter', 'kill_actor', a.name) for a in cls.apps]
        yield multi_async(cmnds)
        
        
class TestTaskQueueMeta(TaskQueueBase, unittest.TestCase):
    
    def test_meta(self):
        '''Tests meta attributes of taskqueue'''
        app = yield get_application(self.apps[0].name)
        self.assertTrue(app)
        self.assertEqual(app.name, self.apps[0].name)
        self.assertFalse(app.cfg.address)
        self.assertTrue(app.backend.registry)
        scheduler = app.scheduler
        self.assertTrue(scheduler.entries)
        job = app.backend.registry['runpycode']
        self.assertEqual(job.type, 'regular')
        self.assertTrue(job.can_overlap)
        id = job.make_task_id((),{})
        self.assertTrue(id)
        self.assertNotEqual(id,job.make_task_id((),{}))
        
    def test_rpc_meta(self):
        app = yield get_application(self.apps[1].name)
        self.assertTrue(app)
        self.assertEqual(app.name, self.apps[1].name)
        self.assertEqual(app.cfg.address, ('127.0.0.1', 0))
        self.assertNotEqual(app.cfg.address, app.address)
        router = app.callable.middleware
        self.assertTrue(router.post)
        root = router.post
        tq = root.taskqueue
        self.assertEqual(tq, self.apps[0].name)
        
    def test_registry(self):
        app = yield get_application(self.apps[0].name)
        self.assertTrue(isinstance(app.backend.registry, dict))
        regular = app.backend.registry.regular()
        periodic = app.backend.registry.periodic()
        self.assertTrue(regular)
        self.assertTrue(periodic)
        
    def test_id_not_overlap(self):
        '''Check `make_task_id` when `can_overlap` attribute is set to False.'''
        app = yield get_application(self.apps[0].name)
        job = app.backend.registry['notoverlap']
        self.assertEqual(job.type, 'regular')
        self.assertFalse(job.can_overlap)
        #
        id = job.make_task_id((),{})
        self.assertTrue(id)
        self.assertEqual(id, job.make_task_id((),{}))
        #
        id = job.make_task_id((10,'bla'),{'p':45})
        self.assertTrue(id)
        self.assertEqual(id,job.make_task_id((10,'bla'),{'p':45}))
        #
        id = job.make_task_id((),{'p':45,'c':'bla'})
        self.assertTrue(id)
        self.assertEqual(id,job.make_task_id((),{'p':45,'c':'bla'}))
        self.assertNotEqual(id,job.make_task_id((),{'p':45,'d':'bla'}))
        self.assertNotEqual(id,job.make_task_id((),{'p':45,'c':'blas'}))
        
        
@sequential
class TestTaskQueueOnThread(TaskQueueBase, unittest.TestCase):

    def _test_not_overlap(self):
        app = yield get_application(self.apps[0].name)
        self.assertTrue('notoverlap' in app.backend.registry)
        r1 = yield app.scheduler.run('notoverlap', 1)
        self.assertEqual(str(r1), 'notoverlap(%s)' % r1.id)
        self.assertTrue(r1._queued)
        r2 = app.scheduler.run('notoverlap', 1)
        self.assertFalse(r2._queued)
        self.assertEqual(r1.id, r2.id)
        # We need to make sure the first task is completed
        r1 = yield app.scheduler.wait_for_task(r1)
        self.assertEqual(get_task(id).status, tasks.SUCCESS)
        
    def test_not_overlap(self):
        return self._test_not_overlap()
        
    @run_on_arbiter
    def test_not_overlap_on_arbiter(self):
        return self._test_not_overlap()
    
    def test_rpc_job_list(self):
        jobs = yield self.proxy.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')

    @run_on_arbiter
    def test_check_next_run(self):
        app = yield get_application(self.apps[0].name)
        scheduler = app.scheduler
        scheduler.tick()
        self.assertTrue(scheduler.next_run > datetime.now())
        
    @run_on_arbiter
    def test_delete_task(self):
        app = yield get_application(self.apps[0].name)
        r1 = yield app.scheduler.run('addition', 1, 4)
        r1 = yield app.acheduler.wait_for_task(r1)
        id = r1.id
        get_task = app.scheduler.get_task
        while get_task(id).status in tasks.UNREADY_STATES:
            yield NOT_DONE
        r2 = get_task(id)
        self.assertEqual(r1.id, r2.id)
        r2 = get_task(id, remove=True)
        self.assertEqual(get_task(id), None)
        self.assertEqual(get_task(r1), r1)
        app.scheduler.delete_tasks()
        
    def test_rpc_ping(self):
        yield self.async.assertEqual(self.proxy.ping(), 'pong')
        
    def test_rpc_job_list(self):
        jobs = yield self.proxy.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')
        
    def test_rpc_job_list_with_names(self):
        jobs = yield self.proxy.job_list(jobnames=['runpycode'])
        self.assertEqual(len(jobs), 1)
        jobs = yield self.proxy.job_list(jobnames=['xxxxxx'])
        self.assertEqual(len(jobs), 0)
        
    def test_rpc_next_scheduled_tasks(self):
        next = yield self.proxy.next_scheduled_tasks()
        self.assertTrue(next)
        self.assertEqual(len(next), 2)
        next = yield self.proxy.next_scheduled_tasks(jobnames=['testperiodic'])
        self.assertTrue(next)
        self.assertEqual(len(next), 2)
        self.assertEqual(next[0], 'testperiodic')
        self.assertTrue(next[1] >= 0)
        
    def test_run_new_task_error(self):
        yield self.async.assertRaises(rpc.InvalidParams,
                            self.proxy.run_new_task())
        yield self.async.assertRaises(rpc.InternalError,
                            self.proxy.run_new_task(jobname='xxxx', bla='foo'))
        
    def test_run_new_task_RunPyCode(self):
        '''Run a new task from the *runpycode* task factory.'''
        r = yield self.proxy.run_new_task(jobname='runpycode', code=CODE_TEST, N=3)
        self.assertTrue(r)
        self.assertTrue(r['id'])
        self.assertTrue(r['time_executed'])
        r = yield wait_for_task(self.proxy, r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertEqual(r['result'], 9)
        
    def test_run_new_task_addition(self):
        r = yield self.proxy.run_new_task(jobname='addition', a=40, b=50)
        self.assertTrue(r)
        self.assertTrue(r['time_executed'])
        r = yield wait_for_task(self.proxy, r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertEqual(r['result'], 90)
        
    def test_run_new_task_periodicerror(self):
        r = yield self.proxy.run_new_task(jobname='testperiodicerror')
        r = yield wait_for_task(self.proxy, r)
        self.assertEqual(r['status'], tasks.FAILURE)
        self.assertTrue('kaputt' in r['result'])
        
    def test_run_new_task_asynchronous(self):
        response = yield self.proxy.run_new_task(jobname='asynchronous',loops=3)
        response = yield wait_for_task(self.proxy, response)
        self.assertEqual(response['status'], tasks.SUCCESS)
        result = response['result']
        self.assertEqual(result['loops'], 3)
        self.assertEqual(result['end']-result['start'], 3)
        
    def test_run_new_task_expiry(self):
        r = yield self.proxy.run_new_task(jobname='addition', a=40, b=50,
                                          meta_data={'expiry': time()})
        self.assertTrue(r)
        r = yield wait_for_task(self.proxy, r)
        self.assertEqual(r['status'], tasks.REVOKED)
        
    def test_run_producerconsumer(self):
        '''A task which produce other tasks'''
        sample = 10
        r = yield self.proxy.run_new_task(jobname='standarddeviation',
                                          sample=sample, size=100)
        self.assertTrue(r)
        r = yield wait_for_task(self.proxy, r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertTrue(tasks.nice_task_message(r))
        # We check for the tasks created
        stasks = []
        while len(stasks) < sample:
            ts = yield self.proxy.get_tasks(from_task=r['id'])
            stasks = []
            for t in ts:
                if t['status'] not in tasks.UNREADY_STATES:
                    stasks.append(t)
                    self.assertEqual(t['status'], tasks.SUCCESS)
        self.assertEqual(len(stasks), sample)
    def test_kill_task_workers(self):
        info = yield self.proxy.server_info()
        tq = info['monitors'][self.apps[0].name]
        for worker in tq['workers']:
            a = worker['actor']
            aid = a['actor_id']
            self.assertEqual(a['is_process'], self.concurrency=='process')
            r = yield self.proxy.kill_actor(aid)
            self.assertTrue(r)


@dont_run_with_thread
class TestTaskQueueOnProcess(TestTaskQueueOnThread):
    concurrency = 'process'
