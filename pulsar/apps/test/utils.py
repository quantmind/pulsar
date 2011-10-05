import sys
import unittest

import pulsar


class _Outcome(object):
    def __init__(self):
        self.success = True
        self.skipped = None
        self.unexpectedSuccess = None
        self.expectedFailure = None
        self.errors = []
        self.failures = []


class TestRequest(object):
    
    def __init__(self, testcls):
        self.testcls = testcls
        
    def run(self, worker):
        loader = unittest.TestLoader()
        self.tests = tests = self.testcls()
        self.test_results = results = tests.defaultTestResult()
        init = getattr(tests,'initTests',None)
        end = getattr(tests,'endTests',None)
        if init:
            yield init()
        for test in loader.loadTestsFromTestCase(self.testcls):
            yield run_test(test,results)
        if end:
            yield end()

    def response(self):
        return self
    
    def close(self):
        pass
    

def run_test(self, result):
    testMethod = getattr(self, self._testMethodName)
    if (getattr(self.__class__, "__unittest_skip__", False) or
        getattr(testMethod, "__unittest_skip__", False)):
        # If the class or method was skipped.
        try:
            skip_why = (getattr(self.__class__, '__unittest_skip_why__', '')
                        or getattr(testMethod, '__unittest_skip_why__', ''))
            self._addSkip(result, skip_why)
        finally:
            result.stopTest(self)
        raise StopIteration
    
    try:
        outcome = _Outcome()
        self._outcomeForDoCleanups = outcome

        yield _executeTestPart(self, self.setUp, outcome)
        if outcome.success:
            yield self._executeTestPart(self, testMethod, outcome, isTest=True)
            yield self._executeTestPart(self, self.tearDown, outcome)

        self.doCleanups()
        if outcome.success:
            result.addSuccess(self)
        else:
            if outcome.skipped is not None:
                self._addSkip(result, outcome.skipped)
            for exc_info in outcome.errors:
                result.addError(self, exc_info)
            for exc_info in outcome.failures:
                result.addFailure(self, exc_info)
            if outcome.unexpectedSuccess is not None:
                addUnexpectedSuccess = getattr(result, 'addUnexpectedSuccess', None)
                if addUnexpectedSuccess is not None:
                    addUnexpectedSuccess(self)
                else:
                    warnings.warn("TestResult has no addUnexpectedSuccess method, reporting as failures",
                                  RuntimeWarning)
                    result.addFailure(self, outcome.unexpectedSuccess)

            if outcome.expectedFailure is not None:
                addExpectedFailure = getattr(result, 'addExpectedFailure', None)
                if addExpectedFailure is not None:
                    addExpectedFailure(self, outcome.expectedFailure)
                else:
                    warnings.warn("TestResult has no addExpectedFailure method, reporting as passes",
                                  RuntimeWarning)
                    result.addSuccess(self)

    finally:
        result.stopTest(self)
    

def _executeTestPart(self, function, outcome, isTest=False):
    try:
        function()
    except KeyboardInterrupt:
        raise
    except SkipTest as e:
        outcome.success = False
        outcome.skipped = str(e)
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
