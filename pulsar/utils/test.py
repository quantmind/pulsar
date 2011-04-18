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

TextTestRunner = unittest.TextTestRunner


logger = logging.getLogger()

LOGGING_MAP = {1: logging.CRITICAL,
               2: logging.INFO,
               3: logging.DEBUG}


class Silence(logging.Handler):
    def emit(self, record):
        pass


def setup_logging(verbosity):
    level = LOGGING_MAP.get(verbosity,None)
    if level is None:
        logger.addHandler(Silence())
    else:
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(level)



class callable_object(object):
    
    def __init__(self,obj,method):
        self.obj = obj
        self.method = method
        
    def __call__(self):
        getattr(self.obj,self.method)()
        
class RunInProcess(object):
    
    def __init__(self,target,args,kwargs):
        self.failed = False
        self.done = False
        self.result = None
        target = self._wrap(target)
        self.p = Process(target=target,args=args,kwargs=kwargs)
        self.p.run()
    
    def _wrap(self, target):
        def _(*args,**kwargs):
            try:
                self.result = target(*args,**kwargs)
            except Exception as e:
                self.failed = e
            finally:
                self.done = True
        return _
    
    def wait(self, timeout = 5):
        tim = time.time()
        while tim - time.time() < timeout:
            if self.done:
                break
            time.sleep(timeout*0.1)
        if self.failed:
            raise self.failed
        return self.result
    

class TestCase(unittest.TestCase):
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
    
    def runInProcess(self,obj,method,*args,**kwargs):
        '''Run the target function into the main process'''
        target = callable_object(obj,method)
        self._suite.writer.send((target,args,kwargs))
        

    def initTest(self):
        pass
    
    def endTest(self):
        pass
    

class TestRunnerThread(Thread):
    
    def __init__(self, suite, verbosity):
        Thread.__init__(self)
        self.verbosity = verbosity
        self.suite = suite
    
    def run(self):
        self.result = TextTestRunner(verbosity = self.verbosity).run(self.suite)
        ioloop = MainIOLoop.instance()
        ioloop.stop()
        
        
class TestSuite(unittest.TestSuite):
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
            init = getattr(obj,'initTest',None)
            if init:
                init()
            for t in test['tests']:
                t(result)
            end = getattr(obj,'endTest',None)
            if end:
                end()
        return result
        
        
        
class TestLoader(object):
    suiteClass = TestSuite
    
    def __init__(self, tags, testtype, extractors, itags):
        self.tags = tags
        self.testtype = testtype
        self.extractors = extractors
        self.itags = itags
        
    def load(self, suite):
        """Return a suite of all tests cases contained in the given module"""
        itags = self.itags or []
        tests = []
        for module in self.modules():
            for name in dir(module):
                obj = getattr(module, name)
                if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
                    tag = getattr(obj,'tag',None)
                    if tag and not tag in itags:
                        continue
                    obj._suite = suite
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
            
            
class TestingMixin(object):
    '''A Test suite which runs tests on a separate process while keeping the main process
busy with the main event loop.'''
    def __init__(self, tags, testtype, extractors, verbosity, itags, writer):
        self.verbosity = verbosity
        self.writer = writer
        self.loader = TestLoader(tags, testtype, extractors, itags)
        self.ioloop = MainIOLoop.instance()
        
    def setup_test_environment(self):
        pass
    
    def teardown_test_environment(self):
        pass
    
    def run(self):
        self.setup_test_environment()
        self.suite = self.loader.load(self)
        result = None
        try:
            result = TextTestRunner(verbosity = self.verbosity).run(self.suite)
        finally:
            self.teardown_test_environment()
            self.result = result
    

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
    
    
class TestSuiteRunner(Process):
    TestingRunner = TestingThread
    #TestingRunner = TestingProcess
    
    def __init__(self, tags, testtype, extractors, verbosity = 1, itags = None):
        setup_logging(verbosity)
        self.connection, connection = Pipe()
        self.runner = self.TestingRunner(tags, testtype, extractors, verbosity, itags, connection)
        self.ioloop = MainIOLoop.instance()
        self.ioloop.add_callback(self.runner.start)
        self.ioloop.add_loop_task(self)
        
    def run_tests(self):
        self.ioloop.start()
        result = self.runner.result
        if result:
            return len(result.failures) + len(result.errors) 
        
    def __call__(self):
        if hasattr(self.runner,'result'):
            self.ioloop.stop()
        else:
            while self.connection.poll():
                callable,args,kwargs = self.reader.recv()
                callable(*args,**kwargs)
            
