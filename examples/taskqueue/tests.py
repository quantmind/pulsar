from time import time
from datetime import datetime, timedelta

from pulsar import test
from pulsar.http import rpc

from .manage import createTaskQueue, server


CODE_TEST = '''\
def task_function(N):
    return N*N
'''
        

class dummyQueue(list):
    
    def put(self, elem):
        self.append(elem)


class TestTaskQueueMeta(test.TestCase):
    
    def setUp(self):
        self.tq = createTaskQueue(parse_console = False,
                                  concurrency = 'thread')
        
    def testCreate(self):
        tq = self.tq
        self.assertTrue(tq.cfg)
        self.assertTrue(tq.registry)
        scheduler = tq.scheduler
        self.assertTrue(scheduler.entries)
        self.assertTrue(scheduler.next_run <= datetime.now())
        
    def testCodeTask(self):
        '''Here we test the application only, not the queue mechanism implemented by the
monitor and workers.'''
        self.assertTrue('sampletasks.codetask' in self.tq.registry)
        r = self.tq.make_request('sampletasks.codetask',(CODE_TEST,10),{})
        self.assertTrue(r)
        self.assertTrue(r.id)
        self.assertTrue(r.time_executed)
        self.assertFalse(r.time_start)
        self.assertEqual(r.args,(CODE_TEST,10))
        consumer = self.tq.load()
        response, result = consumer.handle_event_task(None,r)
        self.assertTrue(response.time_start)
        self.assertTrue(response.execute2start() > 0)
        self.assertEqual(result,100)
        self.assertFalse(response.time_end)
        consumer.end_event_task(None,response,result)
        self.assertTrue(response.time_end)
        self.assertTrue(response.duration() > 0)
        self.assertEqual(response.result,100)
        
    def testTimeout(self):
        '''we set an expire to the task'''
        self.assertTrue('sampletasks.addition' in self.tq.registry)
        consumer = self.tq.load()
        r = self.tq.make_request('sampletasks.codetask',(3,6),expires=time())
        response, result = consumer.handle_event_task(None,r)
        self.assertTrue(response.timeout)
        self.assertTrue(response.exception)
        consumer.end_event_task(None,response,result)
        self.assertTrue(response.exception)
        self.assertFalse(response.result)
        
    def testCheckNextRun(self):
        q = dummyQueue()
        scheduler = self.tq.scheduler
        scheduler.tick(q)
        self.assertTrue(scheduler.next_run > datetime.now())
        now = datetime.now() + timedelta(hours = 1)
        scheduler.tick(q,now)
        self.assertTrue(q)
        td = scheduler.next_run - now
            
    def tearDown(self):
        self.tq.stop()
        self.wait(lambda : self.tq.name in self.arbiter.monitors)

        
class TestRunning(test.TestCase):
    
    def setUp(self):
        self.tq = createTaskQueue(parse_console = False,
                                  concurrency = 'thread')
        
    def testRunning(self):
        self.tq.start()
        self.sleep(1)
        ff = self.tq.scheduler.entries['sampletasks.fastandfurious']
        nr = ff.total_run_count
        self.assertTrue(nr)
        self.sleep(1)
        self.assertTrue(ff.total_run_count > nr)
        
    def tearDown(self):
        self.tq.stop()


class TestTaskRpc(test.TestCase):
    
    def initTests(self):
        s = self.__class__._server = server(bind = '127.0.0.1:0',
                                            concurrency = 'process',
                                            parse_console = False)
        s.start()
        monitor = self.arbiter.monitors[s.mid]
        self.wait(lambda : not monitor.is_alive())
        self.__class__.address = 'http://{0}:{1}'.format(*monitor.address)
        
    def setUp(self):
        self.p = rpc.JsonProxy(self.__class__.address)
        
    def testActorLinks(self):
        s = self._server
        monitor = self.arbiter.monitors[s.mid]
        self.assertTrue(monitor.actor_links)
        app = monitor.actor_links['taskqueue']
        self.assertTrue(app.mid in self.arbiter.monitors)
        tmonitor = self.arbiter.monitors[app.mid]
        self.assertEqual(app,tmonitor.app)
        
    def testPing(self):
        r = self.p.ping()
        self.assertEqual(r,'pong')
            
    #def testEvalCode(self):
    #    r = self.p.evalcode(CODE_TEST,10)
    #    self.assertEqual(r,100)
    
    def endTests(self):
        monitor = self.arbiter.monitors[self._server.mid]
        monitor.stop()
        self.wait(lambda : monitor.aid in self.arbiter.monitors)
        self.assertFalse(monitor.is_alive())
        self.assertTrue(monitor.closed())
    
    
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
        
