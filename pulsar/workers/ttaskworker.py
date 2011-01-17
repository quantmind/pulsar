import gunicorn.workers.base as base


class Worker(base.WorkerThread):
    
    def run(self):
        pass