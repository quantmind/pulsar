import unittest
import inspect
from threading import Thread

from pulsar.utils.eventloop import MainIOLoop

TextTestRunner = unittest.TextTestRunner
TestSuite = unittest.TestSuite


class TestCase(unittest.TestCase):
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
        
        
class TestSuiteRunner(object):
    '''A suite runner with twisted if available.'''
    
    def __init__(self, verbosity = 1, itags = None):
        self.verbosity = verbosity
        self.itags = itags
        self.ioloop = MainIOLoop.instance()
        
    def setup_test_environment(self):
        pass
    
    def teardown_test_environment(self):
        pass
    
    def run_tests(self, modules):
        self.setup_test_environment()
        suite = self.build_suite(modules)
        result = self.run_suite(suite)
        self.teardown_test_environment()
        return self.suite_result(result)
    
    def build_suite(self, modules):
        loader = TestLoader()
        return loader.loadTestsFromModules(modules, itags = self.itags)
        
    def run_suite(self, suite):
        ioloop = self.ioloop
        self.runner = TestRunnerThread(suite,self.verbosity)
        ioloop.add_callback(self.runner.start)
        ioloop.start()
        return self.runner.result
    
    def suite_result(self, result):
        return len(result.failures) + len(result.errors) 
    
    
class TestLoader(unittest.TestLoader):
    suiteClass = TestSuite
    
    def loadTestsFromModules(self, modules, itags = None):
        """Return a suite of all tests cases contained in the given module"""
        itags = itags or []
        tests = []
        for module in modules:
            for name in dir(module):
                obj = getattr(module, name)
                if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
                    tag = getattr(obj,'tag',None)
                    if tag and not tag in itags:
                        continue
                    tests.append(self.loadTestsFromTestCase(obj))
        return self.suiteClass(tests)
    
