from datetime import datetime

from pulsar import test

from .manage import createTaskQueue


class TestTaskQueue(test.TestCase):
    
    def setUp(self):
        self.tq = createTaskQueue(parse_console = False)
        
    def testCreate(self):
        tq = createTaskQueue(parse_console = False)
        self.assertTrue(tq.cfg)
        self.assertTrue(tq.registry)
        scheduler = tq.scheduler
        self.assertTrue(scheduler.entries)
        self.assertTrue(scheduler.next_run <= datetime.now())
        
    def tearDown(self):
        pass


class __TeskTasks(test.TestCase):
        
    def standardchecks(self, request):
        self.assertEqual(request.task_modules,self.controller.task_modules)
        info = request.info
        while not info.time_end:
            self.sleep(0.1)
        self.assertTrue(info.time_executed<=info.time_start)
        self.assertTrue(info.time_start<=info.time_end)
        return info
        
    def _testAddition(self):
        request = self.dispatch('addition',3,4)
        info = self.standardchecks(request)
        self.assertEqual(info.result,7)
        
    def _testException(self):
        request = self.dispatch('valueerrortask')
        info = self.standardchecks(request)
        self.assertTrue('ValueError' in info.exception)
        
        
class __TestSchedulerEntry(TestCase):
    
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
        
        
        
        