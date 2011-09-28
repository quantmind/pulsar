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
        # The config is available. Load the tests
        test_type = self.cfg.test_type
        modules = getattr(self,'modules',None)
        if not modules:
            raise ValueError('No modules specified. Please pass the modules\
 parameters to the TestSuite Constructor.')
        loader = TestLoader(os.getcwd(),modules,test_type)
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
        '''When the monitor starts load all tests classes\
 in the taskqueue'''
        for _,test in self.tests:
            monitor.task_queue.put(test)
            
    def handle_event_task(self, worker, testcls):
        return pulsar.make_async(run_test_case(worker,testcls))            
        
    def __worker_start(self, worker):
        try:
            cfg = self.cfg
            suite =  TestLoader(cfg.labels, cfg.test_type,
                                self.extractors).load(worker)
            verbosity = TestVerbosity(self.loglevel)
            if self.loglevel is not None:
                stream = StreamLogger(worker.log)
                producer = TextTestRunner(stream = stream,
                                          verbosity = verbosity)
            else:
                producer = TextTestRunner(verbosity = verbosity)
            self.producer = producer.run(suite)
            self.producers = []
        except Exception as e:
            raise e.__class__('Could not start tests. {0}'.format(e),
                                exc_info = True)
    
    def __worker_task(self, worker):
        while self.producer:
            try:
                p = next(self.producer)
                if inspect.isgenerator(p):
                    self.producers.append(self.producer)
                    self.producer = p
            except StopIteration:
                if self.producers:
                    self.producer = self.producers.pop()
                else:
                    self.producer = None
        worker.shut_down()
                      
        
def TestSuiteRunner(extractors):
    return TestApplication(extractors = extractors)
    
