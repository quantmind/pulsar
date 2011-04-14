from pulsar import test
from pulsar.apps.tasks import TaskApplication


class TestTaskApplication(test.TestCase):
    
    def testConstruct(self):
        t = TaskApplication()
        self.assertTrue(t.cfg)