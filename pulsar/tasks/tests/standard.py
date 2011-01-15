import time
from .base import TestCase



class TeskTasks(TestCase):
    
    def dispatch(self, name, *args, **kwargs):
        name = 'unuk.contrib.tasks.tests.tlib.other.%s' % name
        return self.controller.dispatch(name, *args, **kwargs)
        
    def standardchecks(self, request):
        self.assertEqual(request.task_modules,self.controller.task_modules)
        info = request.info
        while not info.time_end:
            time.sleep(0.1)
        self.assertTrue(info.time_executed<=info.time_start)
        self.assertTrue(info.time_start<=info.time_end)
        return info
        
    def testAddition(self):
        request = self.dispatch('addition',3,4)
        info = self.standardchecks(request)
        self.assertEqual(info.result,7)
        
    def testException(self):
        request = self.dispatch('valueerrortask')
        info = self.standardchecks(request)
        self.assertTrue('ValueError' in info.exception)
        
        
        