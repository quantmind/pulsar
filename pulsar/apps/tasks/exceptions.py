import pulsar

__all__ = ['TaskException',
           'TaskNotAvailable',
           'TaskTimeout',
           'SchedulingError']


class TaskException(pulsar.PulsarException):
    pass


class TaskNotAvailable(TaskException):
    MESSAGE = 'Task {0} is not registered. Check your settings.'
    def __init__(self, task_name):
        super(TaskNotAvailable,self).__init__(self.MESSAGE.format(task_name))


class TaskTimeout(TaskException):
    MESSAGE = 'Task {0} timed-out (timeout was {1}).'
    def __init__(self, task, timeout):
        super(TaskTimeout,self).__init__(self.MESSAGE.format(task,timeout))


class SchedulingError(TaskException):
    pass