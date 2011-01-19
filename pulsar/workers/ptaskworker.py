
import gunicorn.workers.base as base



class Worker(base.WorkerProcess):
    '''A Task worker on a daemonic subprocess'''
    def run(self):
        pass