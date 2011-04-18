import unittest
import logging
import os
import sys
import time
import inspect
from multiprocessing import Process, Pipe
from threading import Thread

from pulsar.utils.eventloop import MainIOLoop
from pulsar.utils.importer import import_module
from pulsar.utils.defer import RemoteProxyObject 

TextTestRunner = unittest.TextTestRunner


logger = logging.getLogger()

LOGGING_MAP = {1: logging.CRITICAL,
               2: logging.INFO,
               3: logging.DEBUG}


class Silence(logging.Handler):
    def emit(self, record):
        pass


def setup_logging(verbosity):
    '''Setup logging'''
    level = LOGGING_MAP.get(verbosity,None)
    if level is None:
        logger.addHandler(Silence())
    else:
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(level)
    

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
                
    def run(self, result):
        for test in self:
            if result.shouldStop:
                break
            obj = test['obj']
            init = getattr(obj,'initTests',None)
            if init:
                init()
            for t in test['tests']:
                t(result)
            end = getattr(obj,'endTests',None)
            if end:
                end()
        return result
        
        
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
            
    
class SuiteProxy(RemoteProxyObject):
    remotes = ('result','run')
    
    def proxy_run(self, response):
        '''The callback from the main process'''
        

class TestProxy(RemoteProxyObject):
    
    def proxy_result(self, result):
        self.obj.result = result
        self.obj.ioloop.stop()
        
    def proxy_run(self, method, *args, **kwargs):
        result = method(*args,**kwargs)
        return result
        
            
class TestingMixin(object):
    '''A Test suite which runs tests on a separate process while keeping the main process
busy with the main event loop.'''
    def __init__(self, tags, testtype, extractors, verbosity, itags, connection):
        self.verbosity = verbosity
        self.suiterunner = connection
        self.loader = TestLoader(tags, testtype, extractors, itags)
        
    def setup_test_environment(self):
        pass
    
    def teardown_test_environment(self):
        pass
    
    def run(self):
        self.suiterunner = SuiteProxy(self,self.suiterunner)
        self.setup_test_environment()
        self.suite = self.loader.load(self.suiterunner)
        result = None
        try:
            result = TextTestRunner(verbosity = self.verbosity).run(self.suite)
        finally:
            self.teardown_test_environment()
            if result:
                result = len(result.failures) + len(result.errors)
            self.suiterunner.result(result)
    

class TestingProcess(TestingMixin,Process):
    
    def __init__(self, *args):
        TestingMixin.__init__(self, *args)
        Process.__init__(self)
        self.daemon = True
        
        
class TestingThread(TestingMixin,Thread):
    
    def __init__(self, *args):
        TestingMixin.__init__(self, *args)
        Thread.__init__(self)
        self.daemon = True
        
    
class TestSuiteRunner(object):
    # TestingRunner is a class where tests are run.
    # Choose between a Thread or a Process.
    TestingRunner = TestingThread
    #TestingRunner = TestingProcess
    
    def __init__(self, tags, testtype, extractors, verbosity = 1, itags = None):
        setup_logging(verbosity)
        # Pipe for connection
        a, connection = Pipe()
        self.testproxy = TestProxy(self,a)
        self.runner = self.TestingRunner(tags, testtype, extractors,
                                         verbosity, itags, connection)
        self.ioloop = MainIOLoop.instance()
        self.ioloop.add_callback(self.runner.start)
        self.ioloop.add_loop_task(self)
        
    def run_tests(self):
        self.ioloop.start()
        return self.result 
        
    def __call__(self):
        self.testproxy.flush()
            
