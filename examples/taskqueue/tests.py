'''Tests the "taskqueue" example.'''
from time import time, sleep
from datetime import datetime, timedelta

from pulsar import send, get_application, get_actor, NOT_DONE
from pulsar.apps import tasks, rpc
from pulsar.utils.timeutils import timedelta_seconds
from pulsar.apps.test import unittest, run_on_arbiter, dont_run_with_thread

from .manage import server


CODE_TEST = '''\
import time
def task_function(N = 10, lag = 0.1):
    time.sleep(lag)
    return N*N
'''

class TestTaskClasses(unittest.TestCase):
    
    def testTask(self):
        self.assertRaises(NotImplementedError, tasks.Task.get_task, None, 1)
        self.assertRaises(NotImplementedError, tasks.Task.save_task, None, 1)
        self.assertRaises(NotImplementedError, tasks.Task.delete_tasks, None, 1)
        task = tasks.Task()
        self.assertFalse(task.on_received())
        self.assertFalse(task.on_start())
        self.assertFalse(task.on_timeout())
        self.assertFalse(task.on_finish())
        

class TestTaskQueueOnThread(unittest.TestCase):
    concurrency = 'thread'
    app = None
    
    @classmethod
    def name_tq(cls):
        return 'testtask_'+cls.concurrency
    
    @classmethod
    def name_rpc(cls):
        return cls.name_tq() + '_rpc'
    
    @classmethod
    def setUpClass(cls):
        # The name of the task queue application
        s = server(cls.name_tq(), bind='127.0.0.1:0',
                   concurrency=cls.concurrency)
        outcome = send('arbiter', 'run', s)
        yield outcome
        app = outcome.result
        cls.app = app
        uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.proxy = rpc.JsonProxy(uri)

    @classmethod
    def tearDownClass(cls):
        if cls.app:
            yield send('arbiter', 'kill_actor', cls.name_tq())
            yield send('arbiter', 'kill_actor', cls.name_rpc())

    @run_on_arbiter
    def testMeta(self):
        '''Tests meta attributes of taskqueue'''
        app = get_application(self.name_tq())
        self.assertTrue(app)
        self.assertEqual(app.name, self.name_tq())
        self.assertTrue(app.registry)
        scheduler = app.scheduler
        self.assertTrue(scheduler.entries)
        job = app.registry['runpycode']
        self.assertEqual(job.type,'regular')
        self.assertTrue(job.can_overlap)
        id = job.make_task_id((),{})
        self.assertTrue(id)
        self.assertNotEqual(id,job.make_task_id((),{}))
        
    @run_on_arbiter
    def testRegistry(self):
        app = get_application(self.name_tq())
        self.assertTrue(isinstance(app.registry, dict))
        regular = app.registry.regular()
        periodic = app.registry.periodic()
        self.assertTrue(regular)
        self.assertTrue(periodic)
        
    @run_on_arbiter
    def testRpcMeta(self):
        app = get_application(self.name_rpc())
        self.assertTrue(app)
        self.assertEqual(app.name, self.name_rpc())
        rpc = app.callable
        root = rpc.handler
        tq = root.taskqueue
        self.assertEqual(tq, self.name_tq())

    @run_on_arbiter
    def testCheckNextRun(self):
        app = get_application(self.name_tq())
        scheduler = app.scheduler
        scheduler.tick(app)
        self.assertTrue(scheduler.next_run > datetime.now())
        
    @run_on_arbiter
    def testNotOverlap(self):
        app = get_application(self.name_tq())
        self.assertTrue('notoverlap' in app.registry)
        r1 = app.scheduler.queue_task(app.monitor, 'notoverlap', (1,), {})
        self.assertEqual(str(r1), 'notoverlap(%s)' % r1.id)
        self.assertTrue(r1._queued)
        r2 = app.scheduler.queue_task(app.monitor, 'notoverlap', (1,), {})
        self.assertFalse(r2._queued)
        id = r1.id
        self.assertEqual(id, r2.id)
        # We need to make sure the first task is completed
        get_task = app.scheduler.get_task
        while get_task(id).status in tasks.UNREADY_STATES:
            yield NOT_DONE
        self.assertEqual(get_task(id).status, tasks.SUCCESS)
    
    @run_on_arbiter    
    def testIdNotOverlap(self):
        '''Check `make_task_id` when `can_overlap` attribute is set to False.'''
        app = get_application(self.name_tq())
        job = app.registry['notoverlap']
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
        
    @run_on_arbiter
    def testDeleteTask(self):
        app = get_application(self.name_tq())
        r1 = app.scheduler.queue_task(app.monitor, 'addition', (1,4), {})
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
        self.assertEqual(self.proxy.ping(), 'pong')
        
    def test_rpc_job_list(self):
        jobs = self.proxy.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')
        jobs = self.proxy.job_list(jobnames=['runpycode'])
        self.assertEqual(len(jobs), 1)
        jobs = self.proxy.job_list(jobnames=['xxxxxx'])
        self.assertEqual(len(jobs), 0)
        
    def test_rpc_next_scheduled_tasks(self):
        next = self.proxy.next_scheduled_tasks()
        self.assertTrue(next)
        self.assertEqual(len(next), 2)
        next = self.proxy.next_scheduled_tasks(jobnames=['testperiodic'])
        self.assertTrue(next)
        self.assertEqual(len(next), 2)
        self.assertEqual(next[0], 'testperiodic')
        self.assertTrue(next[1] >= 0)
        
    def test_run_new_task_Error(self):
        self.assertRaises(rpc.InvalidParams, self.proxy.run_new_task)
        self.assertRaises(rpc.InvalidParams, self.proxy.run_new_task,
                          jobname='xxxx', bla='foo')
        
    def test_run_new_task_RunPyCode(self):
        '''Run a new task from the *runpycode* task factory.'''
        r = self.proxy.run_new_task(jobname='runpycode', code=CODE_TEST, N=3)
        self.assertTrue(r)
        self.assertTrue(r['id'])
        self.assertTrue(r['time_executed'])
        while r['status'] in tasks.UNREADY_STATES:
            yield NOT_DONE
            r = self.proxy.get_task(id=r['id'])
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertEqual(r['result'], 9)
        
    def test_run_new_task_addition(self):
        r = self.proxy.run_new_task(jobname='addition', a=40, b=50)
        self.assertTrue(r)
        self.assertTrue(r['time_executed'])
        while r['status'] in tasks.UNREADY_STATES:
            yield NOT_DONE
            r = self.proxy.get_task(id=r['id'])
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertEqual(r['result'], 90)
        
    def test_run_new_task_periodicerror(self):
        r = self.proxy.run_new_task(jobname='testperiodicerror')
        while r['status'] in tasks.UNREADY_STATES:
            yield NOT_DONE
            r = self.proxy.get_task(id=r['id'])
        self.assertEqual(r['status'], tasks.FAILURE)
        self.assertTrue('kaputt' in r['result'])
        
    def test_run_new_task_asynchronous(self):
        r = self.proxy.run_new_task(jobname='asynchronous', loops=3)
        while r['status'] in tasks.UNREADY_STATES:
            yield NOT_DONE
            r = self.proxy.get_task(id=r['id'])
        self.assertEqual(r['status'], tasks.SUCCESS)
        result = r['result']
        self.assertEqual(result['loops'], 3)
        self.assertEqual(result['end']-result['start'], 3)
        
    def test_run_new_task_expiry(self):
        r = self.proxy.run_new_task(jobname='addition', a=40, b=50,
                                    meta_data={'expiry': time()})
        self.assertTrue(r)
        while r['status'] in tasks.UNREADY_STATES:
            yield NOT_DONE
            r = self.proxy.get_task(id=r['id'])
        self.assertEqual(r['status'], tasks.REVOKED)
        
    def test_run_producerconsumer(self):
        '''A task which produce other tasks'''
        sample = 10
        r = self.proxy.run_new_task(jobname='standarddeviation',
                                    sample=sample, size=100)
        self.assertTrue(r)
        while r['status'] in tasks.UNREADY_STATES:
            yield NOT_DONE
            r = self.proxy.get_task(id=r['id'])
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertTrue(tasks.nice_task_message(r))
        # We check for the tasks created
        stasks = []
        while len(stasks) < sample:
            ts = self.proxy.get_tasks(from_task=r['id'])
            stasks = []
            for t in ts:
                if t['status'] not in tasks.UNREADY_STATES:
                    stasks.append(t)
                    self.assertEqual(t['status'], tasks.SUCCESS)
        self.assertEqual(len(stasks), sample)
        

#class TestTaskRpc(unittest.TestCase):
class TestTaskRpc(object):
    '''Test the Rpc and Taskqueue server, including rpc commands
in the TaskQueueRpcMixin class'''
    concurrency = 'process'
    timeout = 3

    def testTaskQueueLink(self):
        '''Check the task_queue_manager in the rpc handler.'''
        app = self.app
        self.assertEqual(app.name,self._name_rpc)
        callable = app.callable
        self.assertTrue(callable.handler.task_queue_manager)
        task_queue_manager = callable.handler.task_queue_manager
        self.assertEqual(task_queue_manager.name,self._name)
    def testKillTaskWorker(self):
        r = self.p.server_info()
        m = dict(((m['name'],m) for m in r['monitors']))
        tq = m[self._name]
        for worker in tq['workers']:
            aid = worker['aid']
            r = self.p.kill_actor(aid)
            self.assertTrue(r)


@dont_run_with_thread
class TestTaskQueueOnProcess(TestTaskQueueOnThread):
    concurrency = 'process'