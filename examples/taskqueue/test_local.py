'''Tests the taskqueue local backend.'''
from random import random
from time import time
from datetime import datetime, timedelta

from pulsar import send, get_application, multi_async
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
            

class TaskQueueBase(object):
    concurrency = 'thread'
    schedule_periodic = True
    # used for both keep-alive and timeout in JsonProxy
    # long enough to allow to wait for tasks
    rpc_timeout = 500
    apps = ()
    
    @classmethod
    def name(cls):
        return cls.__name__.lower()
    
    @classmethod
    def task_backend(cls):
        return None
    
    @classmethod
    def rpc_name(cls):
        return 'rpc_%s' % cls.name()
    
    @classmethod
    def setUpClass(cls):
        # The name of the task queue application
        s = server(name=cls.name(),
                   rpc_bind='127.0.0.1:0',
                   concurrent_tasks=4,
                   concurrency=cls.concurrency,
                   rpc_concurrency=cls.concurrency,
                   rpc_keep_alive=cls.rpc_timeout,
                   task_backend=cls.task_backend(),
                   script=__file__,
                   schedule_periodic=cls.schedule_periodic)
        cls.apps = yield send('arbiter', 'run', s)
        # make sure the time out is high enough (bigger than test-timeout)
        cls.proxy = rpc.JsonProxy('http://%s:%s' % cls.apps[1].address,
                                  timeout=cls.rpc_timeout)

    @classmethod
    def tearDownClass(cls):
        cmnds = [send('arbiter', 'kill_actor', a.name) for a in cls.apps]
        yield multi_async(cmnds)
        
        
class TestTaskQueueOnThread(TaskQueueBase, unittest.TestCase):

    def test_meta(self):
        '''Tests meta attributes of taskqueue'''
        app = yield get_application(self.name())
        self.assertTrue(app)
        self.assertEqual(app.name, self.name())
        self.assertFalse(app.cfg.address)
        self.assertEqual(app.cfg.concurrent_tasks, 4)
        self.assertEqual(app.backend.backlog, 4)
        self.assertTrue(app.backend.registry)
        self.assertEqual(app.cfg.concurrency, self.concurrency)
        backend = app.backend
        self.assertFalse(backend.entries)
        job = app.backend.registry['runpycode']
        self.assertEqual(job.type, 'regular')
        self.assertTrue(job.can_overlap)
        id = job.make_task_id((),{})
        self.assertTrue(id)
        self.assertNotEqual(id, job.make_task_id((),{}))

    def test_pubsub(self):
        '''Tests meta attributes of taskqueue'''
        app = yield get_application(self.name())
        self.assertFalse(app.backend.local.pubsub)
        pubsub = app.backend.pubsub
        self.assertEqual(app.backend.local.pubsub, pubsub)
        # the pubsub name is the same as the task queue application
        self.assertEqual(pubsub.name, app.name)
        if not self.task_backend():
            self.assertEqual(pubsub.backend.connection_string,
                             'local://?name=%s' % app.name)

    def test_rpc_meta(self):
        app = yield get_application(self.rpc_name())
        self.assertTrue(app)
        self.assertEqual(app.name, self.rpc_name())
        self.assertEqual(app.cfg.address, ('127.0.0.1', 0))
        self.assertNotEqual(app.cfg.address, app.address)
        self.assertEqual(app.cfg.concurrency, self.concurrency)
        wsgi_handler = app.callable.handler
        self.assertEqual(len(wsgi_handler.middleware), 1)
        router = wsgi_handler.middleware[0] 
        self.assertTrue(router.post)
        root = router.post
        tq = root.taskqueue
        self.assertEqual(tq, self.name())
        
    def test_registry(self):
        app = yield get_application(self.name())
        self.assertTrue(isinstance(app.backend.registry, dict))
        regular = app.backend.registry.regular()
        periodic = app.backend.registry.periodic()
        self.assertTrue(regular)
        self.assertTrue(periodic)

    def test_run_new_task_asynchronous_from_test(self):
        app = yield get_application(self.name())
        r = yield app.backend.run('asynchronous', lag=3)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        result = r['result']
        self.assertTrue(result['loops'])
        self.assertTrue(result['time'] > 3)

    def test_run_new_task_asynchronous(self):
        r = yield self.proxy.run_new_task(jobname='asynchronous', lag=3)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        result = r['result']
        self.assertTrue(result['loops'])
        self.assertTrue(result['time'] > 3)
      
    def test_run_new_task_asynchronous_wait_on_test(self):
        app = yield get_application(self.name())
        r = yield self.proxy.run_new_task(jobname='asynchronous', lag=3)
        r = yield app.backend.wait_for_task(r)
        self.assertEqual(r.status, tasks.SUCCESS)
        self.assertTrue(r.result['loops'])
        self.assertTrue(r.result['time'] > 3)
        
    def test_run_new_simple_task_from_test(self):
        app = yield get_application(self.name())
        r = yield app.backend.run('addition', a=1, b=2)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertEqual(r['result'], 3)
    
    def test_run_new_simple_task(self):
        r = yield self.proxy.run_new_task(jobname='addition', a=40, b=50)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertEqual(r['result'], 90)
    
    def test_not_overlap(self):
        sec = 2 + random()
        app = yield get_application(self.name())
        self.assertEqual(app.name, app.backend.name)
        self.assertTrue('notoverlap' in app.backend.registry)
        r1 = yield app.backend.run('notoverlap', sec)
        self.assertTrue(r1)
        r2 = yield app.backend.run('notoverlap', sec)
        self.assertFalse(r2)
        # We need to make sure the first task is completed
        r1 = yield app.backend.wait_for_task(r1)
        self.assertEqual(r1.status, tasks.SUCCESS)
        self.assertTrue(r1.result > sec)

    def test_run_new_task_error(self):
        yield self.async.assertRaises(rpc.InvalidParams,
                            self.proxy.run_new_task())
        yield self.async.assertRaises(rpc.InternalError,
                            self.proxy.run_new_task(jobname='xxxx', bla='foo'))

    def test_run_new_task_run_py_code(self):
        '''Run a new task from the *runpycode* task factory.'''
        r = yield self.proxy.run_new_task(jobname='runpycode', code=CODE_TEST, N=3)
        self.assertTrue(r)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.SUCCESS)
        self.assertEqual(r['result'], 9)

    def test_run_new_task_periodicerror(self):
        r = yield self.proxy.run_new_task(jobname='testperiodicerror')
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.FAILURE)
        self.assertTrue('kaputt' in r['result'])
     
    def test_run_new_task_expiry(self):
        r = yield self.proxy.run_new_task(jobname='addition', a=40, b=50,
                                          meta_data={'expiry': time()})
        self.assertTrue(r)
        r = yield self.proxy.wait_for_task(r)
        self.assertEqual(r['status'], tasks.REVOKED)
           
    def test_delete_task(self):
        app = yield get_application(self.name())
        id = yield app.backend.run('addition', 1, 4)
        r1 = yield app.backend.wait_for_task(id)
        self.assertEqual(r1.result, 5)
        deleted = yield app.backend.delete_tasks([r1.id, 'kjhbkjb'])
        self.assertEqual(len(deleted), 1)
        r1 = yield app.backend.get_task(r1.id)
        self.assertFalse(r1)
      
    def __test_run_producerconsumer(self):
        '''A task which produce other tasks'''
        sample = 10
        r = yield self.proxy.run_new_task(jobname='standarddeviation',
                                          sample=sample, size=100)
        self.assertTrue(r)
        r = yield self.proxy.wait_for_task(r)
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

    ##    RPC TESTS
    def test_rpc_job_list(self):
        jobs = yield self.proxy.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')

    def test_check_next_run(self):
        app = yield get_application(self.name())
        backend = app.backend
        now = datetime.now()
        backend.tick()
        #self.assertTrue(backend.next_run > now)
        
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
        
    def test_id_not_overlap(self):
        '''Check `make_task_id` when `can_overlap` attribute is set to False.'''
        app = yield get_application(self.name())
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


@dont_run_with_thread
class TestTaskQueueOnProcess(TestTaskQueueOnThread):
    concurrency = 'process'
