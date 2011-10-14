import sys
import unittest

import pulsar


__all__ = ['TestRequest','TestCase']


class Outcome(object):
    
    def __init__(self, result = None):
        self.success = True
        self.skipped = None
        self.unexpectedSuccess = None
        self.expectedFailure = None
        self.errors = []
        self.failures = []
        self.add(result)
        
    def add(self, result):
        if pulsar.is_failure(result):
            self.success = False
            self.failureinfo(result)
            
    def failureinfo(self, result):
        pass


class TestRequest(pulsar.WorkerRequest):
    '''A :class:`pulsar.WorkerRequest` class which wraps a test case class'''
    def __init__(self, testcls):
        self.testcls = testcls
        
    def run(self, worker):
        loader = unittest.TestLoader()
        self.tests = tests = self.testcls()
        self.test_results = results = tests.defaultTestResult()
        init = pulsar.async_pair(getattr(tests,'initTests',None))
        end = getattr(tests,'endTests',None)
        if init:
            result,outcome = init()
            yield result
            if outcome.is_failure():
                if end:
                    yield end()
                yield pulsar.raise_failure()
                    
        for test in loader.loadTestsFromTestCase(self.testcls):
            yield self.run_test(test,results)
            
        if end:
            yield end()
        

    def run_test(self, test, result):
        testMethod = getattr(test, test._testMethodName)
        if (getattr(test.__class__, "__unittest_skip__", False) or
            getattr(testMethod, "__unittest_skip__", False)):
            # If the class or method was skipped.
            try:
                skip_why = (getattr(test.__class__, '__unittest_skip_why__', '')
                            or getattr(testMethod, '__unittest_skip_why__', ''))
                test._addSkip(result, skip_why)
            finally:
                result.stopTest(test)
            raise StopIteration
        
        result, setup_outcome = pulsar.async_pair(test.setUp)()
        yield result
        if not setup_outcome.is_failure():
            result, outcome = pulsar.async_pair(testMethod)()
            yield result
            outcome = Outcome(outcome.result)
            result, teardown_outcome = pulsar.async_pair(test.tearDown)()
            yield result
            outcome.add(teardown_outcome.result)
        else:
            outcome = Outcome(outcome.result)
    
        
        if outcome.success:
            result.addSuccess(test)
        else:
            if outcome.skipped is not None:
                test._addSkip(result, outcome.skipped)
            for exc_info in outcome.errors:
                result.addError(test, exc_info)
            for exc_info in outcome.failures:
                result.addFailure(test, exc_info)
            if outcome.unexpectedSuccess is not None:
                addUnexpectedSuccess = getattr(result, 'addUnexpectedSuccess', None)
                if addUnexpectedSuccess is not None:
                    addUnexpectedSuccess(test)
                else:
                    warnings.warn("TestResult has no addUnexpectedSuccess method, reporting as failures",
                                  RuntimeWarning)
                    result.addFailure(test, outcome.unexpectedSuccess)
    
            if outcome.expectedFailure is not None:
                addExpectedFailure = getattr(result, 'addExpectedFailure', None)
                if addExpectedFailure is not None:
                    addExpectedFailure(test, outcome.expectedFailure)
                else:
                    warnings.warn("TestResult has no addExpectedFailure method, reporting as passes",
                                  RuntimeWarning)
                    result.addSuccess(test)

    

@pulsar.async_pair
def _executeTestPart(self, function, outcome, isTest=False):
    try:
        return function()
    except _UnexpectedSuccess:
        exc_info = sys.exc_info()
        outcome.success = False
        if isTest:
            outcome.unexpectedSuccess = exc_info
        else:
            outcome.errors.append(exc_info)
    except _ExpectedFailure:
        outcome.success = False
        exc_info = sys.exc_info()
        if isTest:
            outcome.expectedFailure = exc_info
        else:
            outcome.errors.append(exc_info)
    except self.failureException:
        outcome.success = False
        outcome.failures.append(sys.exc_info())
        exc_info = sys.exc_info()
    except:
        outcome.success = False
        outcome.errors.append(sys.exc_info())
            
    
class TestCase(unittest.TestCase):
    '''A specialised test case which offers three
additional functions: i) `initTest` and ii) `endTests`,
called at the beginning and at the end of all tests functions declared
in derived classes. Useful for starting a server to send requests
to during tests. iii) `runInProcess` to run a
callable in the main process.'''
    suiterunner = None
    
    def initTests(self):
        '''Called at the beginning off all tests functions in the class'''
        pass
    
    def endTests(self):
        '''Called at the end off all tests functions in the class'''
        pass
    
    @property    
    def arbiter(self):
        return pulsar.arbiter()
        
    def sleep(self, timeout):
        time.sleep(timeout)
        
    def Callback(self):
        return TestCbk()

    def initTests(self):
        pass
    
    def endTests(self):
        pass
    
    def stop(self, a):
        '''Stop an actor and wait for the exit'''
        a.stop()
        still_there = lambda : a.aid in self.arbiter.LIVE_ACTORS
        self.wait(still_there)
        self.assertFalse(still_there())
        
    def wait(self, callback, timeout = 5):
        t = time.time()
        while callback():
            if time.time() - t > timeout:
                break
            self.sleep(0.1)
