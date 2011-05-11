import pulsar
import sys
import traceback

__all__ = ['TaskException',
           'TaskNotAvailable',
           'TaskTimeout',
           'SchedulingError',
           'get_traceback']


def get_traceback(log = None):
    exc_info = sys.exc_info()
    return '\n'.join(traceback.format_exception(*exc_info))


class TaskException(pulsar.PulsarException):
    
    def __init__(self, e, log = None):
        msg = str(e)
        if log:
            log.error(msg, exc_info = sys.exc_info())
        self.stack_trace = get_traceback(log = log)
        super(TaskException,self).__init__(str(e))


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