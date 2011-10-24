import sys
import io
import unittest
import pickle
from inspect import istraceback, isclass

from pulsar import async_pair, as_failure, CLEAR_ERRORS, WorkerRequest,\
                    make_async, SafeAsync, Failure


__all__ = ['TestRequest','TestResult']
          

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
            
 
class CallableTest(SafeAsync):
    
    def __init__(self, pcls, class_method, funcname, max_errors, istest):
        super(CallableTest,self).__init__(max_errors)
        self.class_method = class_method
        self.istest = istest
        self.pcls = pcls
        self.funcname = funcname
        
    def __repr__(self):
        return self.funcname
    __str__ = __repr__
    
    def _call(self, actor):
        self.prepare(actor)
        return self.run()
    
    def prepare(self, worker):
        testcls = pickle.loads(self.pcls)
        testcls.worker = worker
        test = None
        if self.class_method:
            test_function = getattr(testcls,self.funcname)
        else:
            test = testcls(self.funcname)
            if self.istest:
                app = worker.app
                for plugin in app.plugins:
                    test = plugin.setup(test,app.cfg) or test
            test_function = getattr(test,self.funcname)
        self.test = test
        self.test_function = test_function
    
    def run(self):
        return self.test_function()
    

def async_arbiter(test, f, max_errors = 1, istest = False):
    '''Check if *test* needs to be run on the arbiter process domain.
It check if the test function *f* has the attribute *run_on_arbiter*
set to ``True``.

:parameter test: Instance of a testcase
:parameter f: function to test
:parameter max_errors: number of allowed errors in generators.
:rtype: an asynchronous pair.
'''
    if f is None:
        return f
    worker = test.worker
    class_method = True
    if not isclass(test):
        class_method = False
        test = test.__class__
    test.worker = None
    try:
        pcls = pickle.dumps(test)
    except:
        f = lambda : Failure(sys.exc_info())
    else:
        c = CallableTest(pcls, class_method, f.__name__, max_errors, istest)
        if getattr(f, 'run_on_arbiter', False):
            f = lambda : worker.arbiter.send(worker, 'run', c)
        else:
            c.prepare(worker)
            f = c.run
    finally:
        test.worker = worker
    return async_pair(f,  max_errors = max_errors)


class TestRequest(WorkerRequest):
    '''A :class:`pulsar.WorkerRequest` class which wraps a test case class
    
.. attribute:: testcls

    A :class:`unittest.TestCase` class to be run on this request.
'''
    def __init__(self, testcls):
        self.testcls = testcls
        
    def __repr__(self):
        return self.testcls.__name__
    __str__ = __repr__
        
    def run(self, worker):
        '''Run tests from the :attr:`testcls`. First it checks if 
a class method ``setUpClass`` is defined. If so it runs it.'''
        loader = unittest.TestLoader()
        results = worker.app.make_result()
        testcls = self.testcls
        testcls.worker = worker
        init = async_arbiter(testcls,getattr(testcls,'setUpClass',None))
        end = async_arbiter(testcls,getattr(testcls,'tearDownClass',None))
        should_stop = False
        
        if init:
            test = self.testcls('setUpClass')
            result,outcome = init()
            yield result
            should_stop = self.add_failure(test, results, outcome.result)

        if not should_stop:            
            for test in loader.loadTestsFromTestCase(testcls):
                results.startTest(test)
                yield self.run_test(test,results)
                results.stopTest(test)
            
        if end:
            result,outcome = end()
            yield result
            self.add_failure(test, results, outcome.result)
        
        del testcls.worker
        
        # Clear errors
        yield CLEAR_ERRORS
        
        # send results to monitor
        worker.monitor.send(worker,'test_result',TestResult(results))
        
    def run_test(self, test, results):
        '''Run a *test* function.'''
        testMethod = getattr(test, test._testMethodName)
        if (getattr(test.__class__, "__unittest_skip__", False) or
            getattr(testMethod, "__unittest_skip__", False)):
            # If the class or method was skipped.
            try:
                reason = (getattr(test.__class__, '__unittest_skip_why__', '')
                            or getattr(testMethod, '__unittest_skip_why__', ''))
                results.addSkip(test, reason)
            except:
                pass
            raise StopIteration
        
        success = True
        if hasattr(test,'_pre_setup'):
            result, outcome = async_arbiter(test,test._pre_setup)()
            yield result
            success = not self.add_failure(test, results, outcome.result)
        
        if success:
            result, outcome = async_arbiter(test,test.setUp)()
            yield result
            if not self.add_failure(test, results, outcome.result):
                # Here we perform the actual test
                result, outcome = async_arbiter(test,testMethod,istest=True)()
                yield result
                success = not self.add_failure(test, results, outcome.result)
                if success:
                    test.result = outcome.result
                result, outcome = async_arbiter(test,test.tearDown)()
                yield result
                if self.add_failure(test, results, outcome.result):
                    success = False
            else:
                success = False
                
        if hasattr(test,'_post_teardown'):
            result, outcome = async_arbiter(test,test._post_teardown)()
            yield result
            if self.add_failure(test, results, outcome.result):
                success = False
    
        if success:
            results.addSuccess(test)

    def add_failure(self, test, results, failure):
        failure = as_failure(failure)
        if failure:
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