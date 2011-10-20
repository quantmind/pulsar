'''\
An asynchronous parallel testing suite :class:`pulsar.Application`.
It is used for testing
pulsar itself and can be used as a test suite for any application.

Create a script on the top level directory of your library,
let's call it ``runtests.py``::

    from pulsar.apps import TestSuite
    
    if __name__ == '__main__':
        TestSuite(description = 'Test suite for my library',
                  modules = ('regression',
                             ('examples','tests'))).start()
        
where the modules is an iterable for discovering test cases. Check the
:attr:`TestSuite.modules` attribute for more details.

In the above example
the test suite will look for all python files in the ``regression`` module
(in a recursive fashion), and for modules called ``tests`` in the `` example``
module.
'''
import unittest
import logging
import os
import sys
import time
import inspect

import pulsar
import pulsar.apps.tasks # Need to import for the task_queue_factory settings

from .config import *
from .case import *
from .loader import *
from .utils import *


class TestSuite(pulsar.Application):
    '''An asynchronous test suite which works like a task queue where each task
is a group of tests specified in a test class.

:parameter modules: An iterable over modules where to look for tests. A module
    can be a string or a two-element tuple. For example::
    
        suite = TestSuite(modules = ('regression',
                                     ('examples','tests')))
                                     
    The loader will look into the ``regression`` module for all files and
    directory, while it will look into the example directory for all
    files or directories called ``tests``.
    
'''
    app = 'test'
    config_options_include = ('timeout','concurrency','workers','loglevel',
                              'worker_class','debug','task_queue_factory',
                              'http_proxy')
    default_logging_level = None
    cfg = {'timeout':300,
           #'concurrency':'thread',
           'workers':1,
           'loglevel':'none'}
    
    def handler(self):
        return self
    
    def get_ioqueue(self):
        #Return the distributed task queue which produces tasks to
        #be consumed by the workers.
        queue = self.cfg.task_queue_factory
        return queue()
    
    def python_path(self):
        #Override the python path so that we put the directory where the script
        #is in the ppython path
        path = os.getcwd()
        if path not in sys.path:
            sys.path.insert(0, path)
            
    def make_result(self):
        r = unittest.TextTestRunner()
        return r._makeResult()
            
    def on_config(self):
        #Whene config is available load the tests and check what type of
        #action is required.
        pulsar.arbiter()
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
        
        self.local['loader'] = loader
        
    def monitor_init(self, monitor):
        pass
        
    def monitor_start(self, monitor):
        # When the monitor starts load all :class:`TestRequest` into the
        # in the :attr:`pulsar.Actor.ioqueue`.
        loader = self.local['loader']
        tags = self.cfg.labels
        self.tests = list(loader.testclasses(tags))
        monitor.cfg.set('workers',min(self.cfg.workers,len(self.tests)))
        self._results = TestResult()
        self._time_start = time.time()
        for _,testcls in self.tests:
            monitor.put(TestRequest(testcls))
    
    def monitor_task(self, monitor):
        #Check if we got all results
        if self._results.count == len(self.tests):
            time_taken = time.time() - self._time_start
            self.results_summary(time_taken)
            monitor.arbiter.stop()
            
    def handle_request(self, worker, request):
        yield request.run(worker)
        yield request.response()
        
    def actor_test_result(self, sender, worker, result):
        self._results.add(result)
    
    def results_summary(self, timeTaken):
        '''Write the summuray of tests results.'''
        res = self.make_result()
        res.getDescription = lambda test : test
        stream = res.stream
        result = self._results
        res.failures = result.failures
        res.errors = result.errors
        res.printErrors()
        run = result.testsRun
        stream.writeln("Ran %d test%s in %.3fs" %
                            (run, run != 1 and "s" or "", timeTaken))
        stream.writeln()

        expectedFails = unexpectedSuccesses = skipped = 0
        results = map(len, (result.expectedFailures,
                            result.unexpectedSuccesses,
                            result.skipped))
        expectedFails, unexpectedSuccesses, skipped = results

        infos = []
        if not result.wasSuccessful():
            stream.write("FAILED")
            failed, errored = map(len, (result.failures, result.errors))
            if failed:
                infos.append("failures=%d" % failed)
            if errored:
                infos.append("errors=%d" % errored)
        else:
            stream.write("OK")
        if skipped:
            infos.append("skipped=%d" % skipped)
        if expectedFails:
            infos.append("expected failures=%d" % expectedFails)
        if unexpectedSuccesses:
            infos.append("unexpected successes=%d" % unexpectedSuccesses)
        if infos:
            stream.writeln(" (%s)" % (", ".join(infos),))
        else:
            stream.write("\n")

        
def TestSuiteRunner(extractors):
    return TestApplication(extractors = extractors)
    
