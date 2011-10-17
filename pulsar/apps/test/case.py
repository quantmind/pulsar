import sys
import io
import unittest

from pulsar import async_pair, is_failure, CLEAR_ERRORS, WorkerRequest


__all__ = ['TestRequest','TestResult']



def add_failure(test, results, failure):
    if is_failure(failure):
        for trace in failure:
            e = trace[1]
            try:
                raise e
            except test.failureException:
                results.addFailure(test, trace)
            except:
                results.addError(test, trace)
        return True
    else:
        return False
        

class TestResult(object):
    
    def __init__(self, result = None):
        if result:
            self.testsRun = result.testsRun
            self._count = 1
            for attrname in ('failures','errors','skipped','expectedFailures',\
                             'unexpectedSuccesses'):
                val = []
                for test,t in getattr(result,attrname,()):
                    doc_first_line = test.shortDescription()
                    if result.descriptions and doc_first_line:
                        c = '\n'.join((str(test), doc_first_line))
                    else:
                        c = str(test)
                    val.append((c,t))
                setattr(self,attrname,val)
        else:
            self.failures = []
            self.errors = []
            self.skipped = []
            self.expectedFailures = []
            self.unexpectedSuccesses = []
            self.testsRun = 0
            self._count = 0
    
    @property
    def count(self):
        return self._count
    
    def add(self, result):
        self._count += 1
        self.testsRun += result.testsRun
        self.failures.extend(result.failures)
        self.errors.extend(result.errors)
        self.skipped.extend(result.skipped)
        self.expectedFailures.extend(result.expectedFailures)
        self.unexpectedSuccesses.extend(result.unexpectedSuccesses)
        
    def wasSuccessful(self):
        "Tells whether or not this result was a success"
        return len(self.failures) == len(self.errors) == 0
            

class TestRequest(WorkerRequest):
    '''A :class:`pulsar.WorkerRequest` class which wraps a test case class'''
    def __init__(self, testcls):
        self.testcls = testcls
        
    def run(self, worker):
        loader = unittest.TestLoader()
        results = worker.app.make_result()
        if not hasattr(results,'skipped'):
            results.skipped = []
        init = async_pair(getattr(self.testcls,'setUpClass',None))
        end = async_pair(getattr(self.testcls,'tearDownClass',None))
        test = self.testcls('run')
        if init:
            result,outcome = init()
            yield result
            if add_failure(test, results, outcome.result):
                if end:
                    result,outcome = init()
                    yield result
                    add_failure(test, results, outcome.result)
                raise StopIteration
                    
        for test in loader.loadTestsFromTestCase(self.testcls):
            test.worker = worker
            results.startTest(test)
            yield self.run_test(test,results)
            results.stopTest(test)
            
        if end:
            result,outcome = init()
            yield result
            add_failure(test, results, outcome.result)
        
        # Clear errors
        yield CLEAR_ERRORS
        
        # send results to monitor
        worker.monitor.send(worker,'test_result',TestResult(results))
        
    def run_test(self, test, results):
        testMethod = getattr(test, test._testMethodName)
        if (getattr(test.__class__, "__unittest_skip__", False) or
            getattr(testMethod, "__unittest_skip__", False)):
            # If the class or method was skipped.
            try:
                skip_why = (getattr(test.__class__, '__unittest_skip_why__', '')
                            or getattr(testMethod, '__unittest_skip_why__', ''))
                test._addSkip(results, skip_why)
            except:
                pass
            raise StopIteration
        
        result, outcome = async_pair(test.setUp)()
        yield result
        if not add_failure(test, results, outcome.result):
            result, outcome = async_pair(testMethod)()
            yield result
            success = not add_failure(test, results, outcome.result)
            result, outcome = async_pair(test.tearDown)()
            yield result
            if add_failure(test, results, outcome.result):
                success = False
        else:
            success = False
    
        if success:
            results.addSuccess(test)
