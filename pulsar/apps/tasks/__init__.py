'''\
A task scheduler application with HTTP-RPC hooks
'''
import os
import pulsar

from .models import *
from .config import *
from .consumer import *

class TaskApplication(pulsar.Application):
    '''A task scheduler with a JSON-RPC hook for remote procedure calls'''
    
    def init(self, parser, opts, args):
        path = os.path.split(os.getcwd())[0]
        if path not in sys.path:
            sys.path.insert(0, path)
            
    def handler(self):
        return TaskConsumer(self.cfg)
        
    
def run():
    TaskApplication("%prog [OPTIONS] APP_MODULE").run()