from datetime import datetime, timedelta

from unuk.utils.timeutils import timedelta_seconds
from unuk.contrib.tasks import PeriodicTask, Controller, get_schedule, registry
from unuk.contrib.tasks import TaskRegistry, anchorDate

from .base import TestCase

PeriodicScheduler = Controller.PeriodicScheduler
SchedulerEntry    = PeriodicScheduler.Entry 

NUM_TASKS = 4
    

class TestPeriodTasks(TestCase):
    
    def _testScheduler(self):
        schedule = self.scheduler
        self.assertEqual(len(schedule),NUM_TASKS)
        for name,entry in schedule.items():
            self.assertEqual(name,entry.name)
            task = registry[name]
            self.assertTrue(isinstance(task,PeriodicTask))
            self.assertEqual(task.anchor,entry.anchor)
            

class TestSchedulerEntry(TestCase):
    
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
        
        
class TestScheduler(TestCase):
    
    def _testNextRun(self):
        self._setup_schedule(AnchoredEveryHour,AnchoredEvery2seconds)
        tick = self.scheduler.tick()
        self.assertTrue(tick)
        