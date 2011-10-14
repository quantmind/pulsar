'''\
An asynchronous testing application for tesing pulsar and pulsar based
applications. It can be run in parallel by specifying the number of test
workers in the command line.

'''
import unittest
import logging
import os
import sys
import time
import inspect

import pulsar
from pulsar.apps import tasks
from pulsar.utils.importer import import_module

from .config import *
from .utils import *
from .loader import *


class TestSuite(tasks.TaskQueue):
    '''An asyncronous test suite which works like a task queue where each task
is a group of tests specified in a test class.'''
    app = 'test'
    config_options_include = ('timeout','concurrency','workers','loglevel',
                              'worker_class','debug','task_queue_factory')
    default_logging_level = None
    cfg = {'timeout':300,
           'concurrency':'thread',
           'workers':1,
           'loglevel':'none'}
    
    def handler(self):
        return self
    
    def python_path(self):
        #Override the python path so that we put the directory where the script
        #is in the ppython path
        path = os.getcwd()
        if path not in sys.path:
            sys.path.insert(0, path)
            
    def on_config(self):
        '''Whene config is available load the tests and check what type of
action is required.'''
        test_type = self.cfg.test_type
        modules = getattr(self,'modules',None)
        if not modules:
            raise ValueError('No modules specified. Please pass the modules\
 parameters to the TestSuite Constructor.')
        loader = TestLoader(os.getcwd(),modules,test_type)
        
        # Listing labels
        if self.cfg.list_labels:
            print('\nTEST LABELS\n')
            for tag,mod in loader.testmodules():
                doc = mod.__doc__
                if doc:
                    tag = '{0} - {1}'.format(tag,doc)
                print(tag)
            print('\n')
            return False
        
        tags = self.cfg.labels
        self.tests = list(loader.testclasses(tags))
        if not self.tests:
            print('Nothing done. No tests available.')
            return False
        self.cfg.set('workers',min(self.cfg.workers,len(self.tests)))
        
    def monitor_start(self, monitor):
        '''When the monitor starts load all :test:`TestRequest` into the\
 in the :attr:`pulsar.Arbiter.ioqueue`.'''
        for _,testcls in self.tests:
            monitor.put(TestRequest(testcls))
            
    def handle_request(self, worker, request):
        yield request.run(worker)
        yield request.response()
                      
        
def TestSuiteRunner(extractors):
    return TestApplication(extractors = extractors)
    
