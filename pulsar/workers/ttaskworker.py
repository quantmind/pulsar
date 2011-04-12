from .base import WorkerThread
from .ptaskworker import get_task_loop


class Worker(WorkerThread):
    '''A Task worker on a daemonic subprocess'''
    
    def get_ioimpl(self):
        return get_task_loop(self)
    