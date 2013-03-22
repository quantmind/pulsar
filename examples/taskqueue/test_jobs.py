'''Tests the "taskqueue" example.'''
from pulsar.apps.tasks import Task
from pulsar.apps.test import unittest


class TestTaskClasses(unittest.TestCase):
    
    def testTask(self):
        self.assertRaises(NotImplementedError, Task.get_task, None, 1)
        self.assertRaises(NotImplementedError, Task.save_task, None, 1)
        self.assertRaises(NotImplementedError, Task.delete_tasks, None, 1)
        task = Task()
        self.assertFalse(task.on_received())
        self.assertFalse(task.on_start())
        self.assertFalse(task.on_timeout())
        self.assertFalse(task.on_finish())
