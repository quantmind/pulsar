
import gunicorn.workers.base as base



class Worker(base.WorkerProcess):
    '''A Task worker on a daemonic subprocess'''    
    def set_listner(self, socket, app):
        pass
    
    def run(self):
        pass