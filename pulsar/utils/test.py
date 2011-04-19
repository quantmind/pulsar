import unittest
import logging
import os
import sys
import time
import inspect

import pulsar
from pulsar.utils.importer import import_module
from pulsar.utils.async import IOLoop, make_deferred, Deferred

TextTestRunner = unittest.TextTestRunner


logger = logging.getLogger()

LOGGING_MAP = {1: logging.CRITICAL,
               2: logging.INFO,
               3: logging.DEBUG}


class Silence(logging.Handler):
    def emit(self, record):
        pass


class TestCase(unittest.TestCase):
    '''A specialised test case which offers three
additional functions:

a) 'initTest' and 'endTests', called at the beginning and at the end
of the tests declared in a derived class. Useful for starting a server
to send requests to during tests.

b) 'runInProcess' to run a callable in the main process.'''
    _suite = None
    
    def __init__(self, methodName=None):
        if methodName:
            self._dummy = False
            super(TestCase,self).__init__(methodName)
        else:
            self._dummy = True
    
    def __repr__(self):
        if self._dummy:
            return self.__class__.__name__
        else:
            return super(TestCase,self).__repr__()
        
    def sleep(self, timeout):
        time.sleep(timeout)
    
    def runInProcess(self,method,*args,**kwargs):
        '''Run the target function into the main process'''
        return self.suiterunner.run(method,*args,**kwargs)        

    def initTests(self):
        pass
    
    def endTests(self):
        pass
    

class TestSuite(unittest.TestSuite):
    '''A test suite for the modified TestCase.'''
    loader = unittest.TestLoader()
    
    def addTest(self, test):
        tests = self.loader.loadTestsFromTestCase(test)
        if tests:
            try:
                obj = test()
            except:
                obj = test
            self._tests.append({'obj':obj,
                                'tests':tests})
    
    def _runtests(self, res, tests, end, result):
        if isinstance(res,Deferred):
            return res.add_callback(lambda x : self._runtests(x,tests,end,result))
        else:
            for t in tests:
                t(result)
            if end:
                end()
            return result
        
    def run(self, result):
        d = make_deferred()
        for test in self:
            if result.shouldStop:
                break
            obj = test['obj']
            init = getattr(obj,'initTests',None)
            if init:
                d.add_callback(lambda r : init())
            end = getattr(obj,'endTests',None)
            tests = test['tests']
            d.add_callback(lambda res : self._runtests(res,tests,end,result))
        return d.wait(1)
        
        
class TestLoader(object):
    '''Load test cases'''
    suiteClass = TestSuite
    
    def __init__(self, tags, testtype, extractors, itags):
        self.tags = tags
        self.testtype = testtype
        self.extractors = extractors
        self.itags = itags
        
    def load(self, suiterunner):
        """Return a suite of all tests cases contained in the given module.
It injects the suiterunner proxy for comunication with the master process."""
        itags = self.itags or []
        tests = []
        for module in self.modules():
            for name in dir(module):
                obj = getattr(module, name)
                if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
                    tag = getattr(obj,'tag',None)
                    if tag and not tag in itags:
                        continue
                    obj.suiterunner = suiterunner
                    tests.append(obj)
        return self.suiteClass(tests)
    
    def get_tests(self,dirpath):
        join  = os.path.join
        loc = os.path.split(dirpath)[1]
        for d in os.listdir(dirpath):
            if d.startswith('__'):
                continue
            if os.path.isdir(join(dirpath,d)):
                yield (loc,d)
            
    def modules(self):
        tags,testtype,extractors = self.tags,self.testtype,self.extractors
        for extractor in extractors:
            testdir = extractor.testdir(testtype)
            for loc,app in self.get_tests(testdir):
                if tags and app not in tags:
                    logger.debug("Skipping tests for %s" % app)
                    continue
                logger.debug("Try to import tests for %s" % app)
                test_module = extractor.test_module(testtype,loc,app)
                try:
                    mod = import_module(test_module)
                except ImportError as e:
                    logger.debug("Could not import tests for %s: %s" % (test_module,e))
                    continue
                
                logger.debug("Adding tests for %s" % app)
                yield mod
        
            
#class TestWorker(pulsar.WorkerProcess):
class TestWorker(pulsar.WorkerThread):
        
    def setup_test_environment(self):
        pass
    
    def teardown_test_environment(self):
        pass
    
    def _run(self):
        cfg = self.cfg
        self.loader = TestLoader(cfg.tags, cfg.testtype, cfg.extractors, cfg.itags)
        self.ioloop = IOLoop()
        self.ioloop.add_loop_task(self)
        self.setup_test_environment()
        self.suite = self.loader.load(self.pool)
        try:
            result = TextTestRunner(verbosity = cfg.verbosity).run(self.suite)
            self.ioloop.start()
        finally:
            self.teardown_test_environment()
            self.ioloop.stop()
    

class TestApplication(pulsar.Application):
    ArbiterClass = pulsar.Arbiter
    
    '''A dummy application for testing'''
    def load_config(self, **params):
        pass
    
    def handler(self):
        return self
    
    def configure_logging(self):
        '''Setup logging'''
        verbosity = self.cfg.verbosity
        level = LOGGING_MAP.get(verbosity,None)
        if level is None:
            logger.addHandler(Silence())
        else:
            logger.addHandler(logging.StreamHandler())
            logger.setLevel(level)
        

class TestConfig(pulsar.DummyConfig):
    '''Configuration for testing'''
    def __init__(self, tags, testtype, extractors, verbosity, itags):
        self.tags = tags
        self.testtype = testtype
        self.extractors = extractors
        self.verbosity = verbosity
        self.itags = itags
        self.worker_class = TestWorker
        self.workers = 1
        
        
def TestSuiteRunner(tags, testtype, extractors, verbosity = 1, itags = None):
    cfg = TestConfig(tags, testtype, extractors, verbosity, itags)
    TestApplication(cfg = cfg).start()

