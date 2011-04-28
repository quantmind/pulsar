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
        self.arbiter.


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
        
        
        