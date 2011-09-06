import pulsar
import sys
import traceback

from .states import REVOKED

__all__ = ['TaskException',
           'TaskNotAvailable',
           'TaskTimeout',
           'SchedulingError',
           'get_traceback']


def get_traceback(log = None):
    exc_info = sys.exc_info()
    return '\n'.join(traceback.format_exception(*exc_info))


class TaskException(pulsar.PulsarException):
    pass


class TaskNotAvailable(TaskException):
    MESSAGE = 'Task {0} is not registered. Check your settings.'
    def __init__(self, task_name):
        super(TaskNotAvailable,self).__init__(self.MESSAGE.format(task_name))


class TaskTimeout(TaskException):
    status = REVOKED


class SchedulingError(TaskException):
    pass
