'''Tests the "taskqueue" example.'''
from pulsar.apps.tasks import Task, TaskBackend
from pulsar.apps.test import unittest


class TestTaskClasses(unittest.TestCase):
    
    def testTaskBackend(self):
        b = TaskBackend('dummy', None)
        self.assertRaises(NotImplementedError, b.get_task, 1)
        self.assertRaises(NotImplementedError, b.save_task, 1)
        self.assertRaises(NotImplementedError, b.delete_tasks, [])
        self.assertRaises(NotImplementedError, b.flush)
        self.assertEqual(b.processed, 0)
        self.assertEqual(b.max_tasks, 0)
