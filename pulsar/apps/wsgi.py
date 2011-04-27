import os
import sys

from pulsar import Application, ThreadQueue
from pulsar.utils.importer import import_app


class WSGIApplication(Application):
    
    def init(self, parser, opts, args):
        if self.callable is None:
            parser.error("No application module specified.")        
        sys.path.insert(0, os.getcwd())

    def load(self):
        return import_app(self.app_uri)
    
    def get_task_queue(self): 
        if self.cfg.concurrency == 'process':
            return None
        else:
            return ThreadQueue()


def createServer(callable = None, **params):
    return WSGIApplication(callable = callable, **params)
    

