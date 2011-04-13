from .base import WorkerThread
from .task import get_task_loop, start_task_loop, TaskMixin


class Worker(TaskMixin,WorkerThread):
    '''A Task worker on a daemonic subprocess'''
    
    def get_ioimpl(self):
        return get_task_loop(self)
    
    def _run(self):
        start_task_loop(self)
        