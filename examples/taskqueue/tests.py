'''Tests the "taskqueue" example.'''
from time import time, sleep
from datetime import datetime, timedelta

import pulsar
from pulsar.apps import tasks, rpc
from pulsar.apps.test import test_server
from pulsar.utils.timeutils import timedelta_seconds
from pulsar.utils.test import test

from .manage import createTaskQueue, server


CODE_TEST = '''\
import time
def task_function(N = 10, lag = 0.1):
    time.sleep(lag)
    return N*N
'''
        

class TestTaskQueueMeta(test.TestCase):
    concurrency = 'process'
    
    @classmethod
    def setUpClass(cls):
        s = test_server(createTaskQueue,
                        concurrency = cls.concurrency,
                        name = 'tq')
        r,outcome = cls.worker.run_on_arbiter(s)
        yield r
        cls.app = outcome.result
        
    @classmethod
    def tearDownClass(cls):
        return cls.worker.arbiter.send(cls.worker,'kill_actor',cls.app.mid)
    
    def tq(self):
        import pulsar
        arbiter = pulsar.arbiter()
        self.assertTrue(len(arbiter.monitors)>=2)
        monitor = arbiter.monitors.get('tq')
        self.assertEqual(monitor.name,'tq')
        self.assertTrue(monitor.running())
        return monitor
        
    def testMeta(self):
        '''Tests meta attributes of taskqueue'''
        tq = self.tq()
        app = tq.app
        self.assertTrue(app.registry)
        scheduler = app.scheduler
        self.assertTrue(scheduler.entries)
        job = app.registry['runpycode']
        self.assertEqual(job.type,'regular')
        self.assertTrue(job.can_overlap)
        id = job.make_task_id((),{})
        self.assertTrue(id)
        self.assertNotEqual(id,job.make_task_id((),{}))
    testMeta.run_on_arbiter = True
    
    def testIdNotOverlap(self):
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
        
    def testNotOverlap(self):
        tq = self.tq()
        app = tq.app
        self.assertTrue('notoverlap' in app.registry)
        r1 = app.scheduler.queue_task(tq, 'notoverlap', (1,), {})
        self.assertFalse(r1.needs_queuing())
        self.assertTrue(r1._queued)
        id = r1.id
        r2 = app.scheduler.queue_task(tq, 'notoverlap', (1,), {})
        self.assertFalse(r2._queued)
        self.assertEqual(id,r2.id)
    testNotOverlap.run_on_arbiter = True
        
    def testApplicationSimple(self):
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
    testApplicationSimple.run_on_arbiter = True
        
    def testApplicationSimpleError(self):
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
    testApplicationSimpleError.run_on_arbiter = True
        
    def testTimeout(self):
        '''we set an expire to the task'''
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
    testTimeout.run_on_arbiter = True    
        
    def testCheckNextRun(self):
        tq = self.tq()
        app = tq.app
        scheduler = app.scheduler
        scheduler.tick(tq)
        self.assertTrue(scheduler.next_run > datetime.now())
        #now = datetime.now() + timedelta(hours = 1)
        #scheduler.tick(tq,now)
    testCheckNextRun.run_on_arbiter = True    
        
    #def testRunning(self):
    #    ff = self.tq.scheduler.entries['sampletasks.fastandfurious']
    #    nr = ff.total_run_count
    #    self.assertTrue(nr)
        #self.sleep(5)
        #self.assertTrue(ff.total_run_count > nr)


class TestTaskRpc(test.TestCase):
    '''Test the Rpc and Taskqueue server, including rpc commands
in the TaskQueueRpcMixin class'''
    concurrency = 'process'
    timeout = 3
    
    @classmethod
    def setUpClass(cls):
        name = 'testtask_'+cls.concurrency
        name_rpc = name + '_rpc'
        s = test_server(server,
                        name = name,
                        bind = '127.0.0.1:0',
                        concurrency = cls.concurrency,
                        parse_console = False)
        r,outcome = cls.worker.run_on_arbiter(s)
        yield r
        cls._name = name
        cls._name_rpc = name_rpc
        cls.app = outcome.result
        cls.uri = 'http://{0}:{1}'.format(*cls.app.address)
        
    @classmethod
    def tearDownClass(cls):
        yield cls.worker.arbiter.send(cls.worker,'kill_actor',cls._name)
        yield cls.worker.arbiter.send(cls.worker,'kill_actor',cls._name_rpc)
        
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
        
    def testJobList(self):
        r = self.p.job_list()
        self.assertTrue(r)
        self.assertTrue(isinstance(r,list))
        d = dict(r)
        pycode = d['runpycode']
        self.assertEqual(pycode['type'],'regular')
        
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
        worker = tq['workers'][0]
        aid = worker['aid']
        r = self.p.kill_actor(aid)
        self.assertTrue(r)
            

class TestTaskRpcThread(TestTaskRpc):
    concurrency = 'thread'
    
    
class TestSchedulerEntry(test.TestCase):
        
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
        
