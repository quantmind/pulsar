from unuk.test import TestCase
from unuk.concurrency import getpool
from unuk.contrib.tasks import Controller
from unuk.contrib.tasks import registry


class TestCase(TestCase):
    withpool = 1
    taskdir = 'unuk.contrib.tasks.tests.tlib'
    
    def setUp(self):
        pool = getpool(self.withpool)
        self.controller = Controller(pool = pool,
                                     task_modules = ['{0}.*'.format(self.taskdir)],
                                     beat = 0.5)
        self.controller.start()
        self.scheduler  = self.controller.scheduler
        self.registry   = registry
        
    def tearDown(self):
        self.controller.wait()
        
        