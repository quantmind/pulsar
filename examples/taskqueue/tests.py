'''Tests the "taskqueue" example.'''
from time import time, sleep
from datetime import datetime, timedelta

from pulsar import send, get_application
from pulsar.apps import tasks, rpc
from pulsar.utils.timeutils import timedelta_seconds
from pulsar.apps.test import unittest, run_on_arbiter

from .manage import server


CODE_TEST = '''\
import time
def task_function(N = 10, lag = 0.1):
    time.sleep(lag)
    return N*N
'''


class TestCase(unittest.TestCase):
    concurrency = 'process'
    app = None

    @classmethod
    def setUpClass(cls):
        # The name of the task queue application
        name_tq = 'testtask_'+cls.concurrency
        name_rpc = name_tq + '_rpc'
        s = server(name_tq, bind ='127.0.0.1:0', concurrency=cls.concurrency)
        outcome = send('arbiter', 'run', s)
        yield outcome
        app = outcome.result
        cls.name_tq = name_tq
        cls.name_rpc = name_rpc
        cls.app = app
        uri = 'http://{0}:{1}'.format(*cls.app.address)
        cls.proxy = rpc.JsonProxy(uri)

    @classmethod
    def tearDownClass(cls):
        if cls.app:
            yield send('arbiter', 'kill_actor', cls.name_tq)
            yield send('arbiter', 'kill_actor', cls.name_rpc)


class TestTaskQueueOnThread(TestCase):
    concurrency = 'thread'

    @run_on_arbiter
    def testMeta(self):
        '''Tests meta attributes of taskqueue'''
        app = get_application(self.name_tq)
        self.assertTrue(app)
        self.assertEqual(app.name, self.name_tq)
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
    def testRpcMeta(self):
        app = get_application(self.name_rpc)
        self.assertTrue(app)
        self.assertEqual(app.name, self.name_rpc)
        rpc = app.callable
        root = rpc.handler
        tq = root.task_queue_manager
        self.assertEqual(tq.actor_link_name, self.name_tq)

    @run_on_arbiter
    def testCheckNextRun(self):
        app = get_application(self.name_tq)
        scheduler = app.scheduler
        scheduler.tick(app)
        self.assertTrue(scheduler.next_run > datetime.now())
        
    @run_on_arbiter
    def testNotOverlap(self):
        app = get_application(self.name_tq)
        self.assertTrue('notoverlap' in app.registry)
        r1 = app.scheduler.queue_task(app.monitor, 'notoverlap', (1,), {})
        self.assertFalse(r1.needs_queuing())
        self.assertTrue(r1._queued)
        id = r1.id
        r2 = app.scheduler.queue_task(app.monitor, 'notoverlap', (1,), {})
        self.assertFalse(r2._queued)
        self.assertEqual(id,r2.id)
        
    def testRpc(self):
        self.assertEqual(self.proxy.ping(), 'pong')
        
    def testRpc_job_list(self):
        jobs = self.proxy.job_list()
        self.assertTrue(jobs)
        self.assertTrue(isinstance(jobs, list))
        d = dict(jobs)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'], 'regular')
        jobs = self.proxy.job_list(jobnames=['runpycode'])
        self.assertTrue(len(jobs), 1)
        
    def __testIdNotOverlap(self):
        '''Check `make_task_id` when `can_overlap` attribute is set to False.'''
        from examples.taskqueue.sampletasks.sampletasks import NotOverLap
        job = NotOverLap()
        self.assertEqual(job.type,'regular')
        self.assertFalse(job.can_overlap)
        #
        id = job.make_task_id((),{})
        self.assertTrue(id)
        self.assertEqual(id,job.make_task_id((),{}))
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

    def __testApplicationSimple(self):
        '''Here we test the application only, not the queue mechanism
implemented by the monitor and workers.'''
        # create a request
        tq = self.tq()
        app = tq.app
        self.assertTrue('runpycode' in app.registry)
        r = app.scheduler.queue_task(tq, 'runpycode', (CODE_TEST,), {'N': 10})
        self.assertTrue(r)
        self.assertTrue(r.id)
        self.assertTrue(r.time_executed)
        self.assertEqual(r.args,(CODE_TEST,))
        self.assertEqual(r.kwargs,{'N': 10})
        while not r.done():
            yield pulsar.NOT_DONE # Give a chance to perform the task
            r = app.task_class.get_task(r.id)
        self.assertTrue(r.time_start)
        self.assertTrue(r.status,tasks.SUCCESS)
        self.assertEqual(r.result,100)
        self.assertTrue(r.time_end)
        self.assertTrue(r.time_end>r.time_start)
        d = timedelta_seconds(r.duration())
        self.assertTrue(d > 0.1)

    def __testApplicationSimpleError(self):
        tq = self.tq()
        app = tq.app
        r = app.scheduler.queue_task(tq, 'runpycode', (CODE_TEST,),{'N': 'bla'})
        self.assertTrue(r)
        self.assertTrue(r.id)
        self.assertTrue(r.time_executed)
        self.assertEqual(r.args,(CODE_TEST,))
        self.assertEqual(r.kwargs,{'N': 'bla'})
        while not r.done():
            yield pulsar.NOT_DONE # Give a chance to perform the task
            r = app.task_class.get_task(r.id)
        self.assertTrue(r.time_start)
        self.assertTrue(r.status,tasks.FAILURE)
        self.assertTrue(r.result.startswith("can't multiply sequence"))
        self.assertTrue(r.time_end)
        self.assertTrue(r.time_end>r.time_start)
        d = timedelta_seconds(r.duration())
        self.assertTrue(d > 0.1)

    def __testTimeout(self):
        '''we set an expire to the task'''
        td = arbiter()
        tq = self.tq()
        app = tq.app
        r = app.scheduler.queue_task(tq, 'runpycode',
                                     (CODE_TEST,),{'N': 2, 'lag': 2},
                                     expiry=time())
        while not r.done():
            yield pulsar.NOT_DONE # Give a chance to perform the task
            r = app.task_class.get_task(r.id)
        self.assertTrue(r.timeout)
        self.assertEqual(r.status,tasks.REVOKED)

    #def testRunning(self):
    #    ff = self.tq.scheduler.entries['sampletasks.fastandfurious']
    #    nr = ff.total_run_count
    #    self.assertTrue(nr)
        #self.sleep(5)
        #self.assertTrue(ff.total_run_count > nr)


#class TestTaskRpc(unittest.TestCase):
class TestTaskRpc(object):
    '''Test the Rpc and Taskqueue server, including rpc commands
in the TaskQueueRpcMixin class'''
    concurrency = 'process'
    timeout = 3

    @classmethod
    def setUpClass(cls):
        name = 'testtask_'+cls.concurrency
        name_rpc = name + '_rpc'
        s = server(name, bind ='127.0.0.1:0', concurrency=cls.concurrency)
        r,outcome = cls.worker.run_on_arbiter(s)
        yield r
        cls._name = name
        cls._name_rpc = name_rpc
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)

    @classmethod
    def tearDownClass(cls):
        yield cls.worker.arbiter.send(cls.worker,'kill_actor',cls._name_rpc)
        yield cls.worker.arbiter.send(cls.worker,'kill_actor',cls._name)

    def setUp(self):
        self.p = rpc.JsonProxy(self.uri, timeout = self.timeout)

    def testPing(self):
        r = self.p.ping()
        self.assertEqual(r,'pong')

    def testTaskQueueLink(self):
        '''Check the task_queue_manager in the rpc handler.'''
        app = self.app
        self.assertEqual(app.name,self._name_rpc)
        callable = app.callable
        self.assertTrue(callable.handler.task_queue_manager)
        task_queue_manager = callable.handler.task_queue_manager
        self.assertEqual(task_queue_manager.name,self._name)

    def testPing(self):
        r = self.p.ping()
        self.assertEqual(r,'pong')

    def testRunPyCode(self):
        r = self.p.runpycode(code = CODE_TEST, N = 3)
        self.assertTrue(r)
        self.assertTrue(r['time_executed'])
        sleep(0.2)
        rr = self.p.get_task(id = r['id'])
        self.assertTrue(rr)
        self.assertEqual(rr['status'],tasks.SUCCESS)
        self.assertEqual(rr['result'],9)
        
    def testRunNewTask(self):
        r = self.p.run_new_task(jobname = 'addition', a = 40, b = 50)
        self.assertTrue(r)
        self.assertTrue(r['time_executed'])
        sleep(0.1)
        rr = self.p.get_task(id = r['id'])
        self.assertTrue(rr)
        self.assertEqual(rr['status'],tasks.SUCCESS)
        self.assertEqual(rr['result'],90)

    def testKillTaskWorker(self):
        r = self.p.server_info()
        m = dict(((m['name'],m) for m in r['monitors']))
        tq = m[self._name]
        for worker in tq['workers']:
            aid = worker['aid']
            r = self.p.kill_actor(aid)
            self.assertTrue(r)


#class TestSchedulerEntry(unittest.TestCase):
class TestSchedulerEntry(object):

    def _testAnchored(self, entry, delta):
        # First test is_due is False
        last_run_at = entry.scheduled_last_run_at
        now = last_run_at + delta
        is_due, next_time_to_run = entry.is_due(now = now)
        self.assertFalse(is_due)
        next_run_at = last_run_at + entry.run_every
        seconds = timedelta_seconds(next_run_at - now)
        self.assertAlmostEqual(next_time_to_run,seconds,2)

        # Second test is_due is True
        now = next_run_at
        is_due, next_time_to_run = entry.is_due(now = now)
        self.assertTrue(is_due)
        self.assertEqual(next_time_to_run,timedelta_seconds(entry.run_every))

        # check the new entry
        new_entry = entry.next(now = now)
        self.assertEqual(new_entry.scheduled_last_run_at,last_run_at + entry.run_every)

    def _testAnchoredEveryHour(self):
        '''Test an hourly anchored task'''
        entry = self._setup_schedule(AnchoredEveryHour)[0]
        self.assertTrue(entry.anchor)

        last_run_at = entry.scheduled_last_run_at
        self.assertEqual(last_run_at.minute,entry.anchor.minute)
        self.assertEqual(last_run_at.second,entry.anchor.second)

        self._testAnchored(entry, timedelta(minutes = 1))

    def _testAnchoredEvery2seconds(self):
        entry = self._setup_schedule(AnchoredEvery2seconds)[0]
        self.assertTrue(entry.anchor)

        last_run_at = entry.scheduled_last_run_at
        mult = int(last_run_at.second/2)
        self.assertEqual(last_run_at.second,mult*2)

        self._testAnchored(entry, timedelta(seconds = 1))

