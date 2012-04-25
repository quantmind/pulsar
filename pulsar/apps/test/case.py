import sys
import io
import pickle
from inspect import istraceback, isclass

from pulsar import async_pair, as_failure, CLEAR_ERRORS, WorkerRequest,\
                    make_async, SafeAsync, Failure


__all__ = ['TestRequest','run_on_arbiter']
          
 
class CallableTest(SafeAsync):
    
    def __init__(self, test, class_method, funcname, max_errors, istest):
        super(CallableTest,self).__init__(max_errors)
        self.class_method = class_method
        self.istest = istest
        self.test = test
        self.funcname = funcname
    
    def __repr__(self):
        return self.funcname
    __str__ = __repr__
    
    def _call(self, actor):
        self.test = pickle.loads(self.test)
        if actor.is_arbiter():
            actor = actor.monitors['testsuite']
        self.test.worker = actor
        self.prepare()
        return self.run()
    
    def prepare(self):
        if self.istest:
            test = self.test
            runner = test.worker.app.runner
            self.test = runner.getTest(test)
            self.test.async = AsyncAssert(self.test)
        self.test_function = getattr(self.test,self.funcname)
    
    def run(self):
        return self.test_function()
    

class AsyncAssert(object):
    __slots__ = ('test','name')
    
    def __init__(self, test, name = None):
        self.test = test
        self.name = name
        
    def __getattr__(self, name):
        return AsyncAssert(self.test,name)
    
    def __call__(self, elem, *args):
        return make_async(elem).add_callback(\
                        lambda r : self._check_result(r,*args))
    
    def _check_result(self, result, *args):
        func = getattr(self.test,self.name)
        return func(result,*args)
    
    
def run_on_arbiter(test, f, max_errors = 1, istest = False):
    '''This internal function is used by the test runner for running a *test*
on the :class:`pulsar.Arbiter` process domain.
It check if the test function *f* has the attribute *run_on_arbiter*
set to ``True``, and if so the test is send to the arbiter. For example::

    class mystest(unittest.TestCase):
        
        def testBla(self):
            ...
        testBla.run_on_arbiter = True

:parameter test: Instance of a testcase
:parameter f: function to test
:parameter max_errors: number of allowed errors in generators.
:rtype: an asynchronous pair (check :func:`pulsar.async_pair` for details)
'''
    try:
        if f is None:
            return f
        class_method = isclass(test)
        if getattr(f, 'run_on_arbiter', False):
            worker = test.worker
            test.worker = None
            try:
                pcls = pickle.dumps(test)
            except:
                f = lambda : Failure(sys.exc_info())
            else:
                c = CallableTest(pcls, class_method, f.__name__,
                                 max_errors, istest)
                f = lambda : worker.arbiter.send(worker, 'run', c)
            finally:
                test.worker = worker
        else:
            c = CallableTest(test, class_method, f.__name__, max_errors, istest)
            c.prepare()
            f = c.run
        return async_pair(f,  max_errors = max_errors)
    except:
        fail = Failure(sys.exc_info())
        return lambda : async_pair(fail, max_errors = max_errors)


class TestRequest(WorkerRequest):
    '''A :class:`pulsar.WorkerRequest` class which wraps a test case class
    
.. attribute:: testcls

    A :class:`unittest.TestCase` class to be run on this request.
    
.. attribute:: tag

    A string indicating the tag associated with :attr:`testcls`.
'''
    def __init__(self, testcls, tag):
        self.testcls = testcls
        self.tag = tag
        
    def __repr__(self):
        return self.testcls.__name__
    __str__ = __repr__
        
    def run(self, worker):
        '''Run all test functions from the :attr:`testcls` using the
following algorithm:

* Run the class method ``setUpClass`` of :attr:`testcls` of if defined.
* Call :meth:`run_test` for each test functions in :attr:`testcls`
* Run the class method ``tearDownClass`` of :attr:`testcls` if defined.  
'''
        # Reset the runner
        worker.app.local.pop('runner', None)
        runner = worker.app.runner
        testcls = self.testcls
        testcls.tag = self.tag
        all_tests = runner.loadTestsFromTestCase(testcls)
        
        if all_tests.countTestCases():
            testcls.worker = worker
            should_stop = False
            end = None
            if not getattr(testcls, "__unittest_skip__", False):
                init = run_on_arbiter(testcls,getattr(testcls,
                                                     'setUpClass',None))
                end = run_on_arbiter(testcls,getattr(testcls,
                                                    'tearDownClass',None))
                should_stop = False
            
                if init:
                    test = self.testcls('setUpClass')
                    result,outcome = init()
                    yield result
                    should_stop = self.add_failure(test, runner, outcome.result)
    
            if not should_stop:            
                for test in all_tests:
                    runner.startTest(test)
                    yield self.run_test(test,runner)
                    runner.stopTest(test)
                
            if end:
                result, outcome = end()
                yield result
                self.add_failure(test, runner, outcome.result)
            
            del testcls.worker
            
            # Clear errors
            yield CLEAR_ERRORS
        
        # send runner result to monitor
        worker.monitor.send(worker,'test_result',runner.result)
        
    def run_test(self, test, runner):
        '''\
Run a *test* function using the following algorithm
        
* Run :meth:`_pre_setup` method if available in :attr:`testcls`.
* Run :meth:`setUp` method in :attr:`testcls`.
* Run the test function.
* Run :meth:`tearDown` method in :attr:`testcls`.
* Run :meth:`_post_teardown` method if available in :attr:`testcls`.
'''
        try:
            success = True
            testMethod = getattr(test, test._testMethodName)
            if (getattr(test.__class__, "__unittest_skip__", False) or
                getattr(testMethod, "__unittest_skip__", False)):
                reason = (getattr(test.__class__,
                                  '__unittest_skip_why__', '') or
                          getattr(testMethod,
                                  '__unittest_skip_why__', ''))
                runner.addSkip(test, reason)
                raise StopIteration()
            
            if hasattr(test,'_pre_setup'):
                result, outcome = run_on_arbiter(test,test._pre_setup)()
                yield result
                success = not self.add_failure(test, runner, outcome.result)
            
            if success:
                result, outcome = run_on_arbiter(test,test.setUp)()
                yield result
                if not self.add_failure(test, runner, outcome.result):
                    # Here we perform the actual test
                    result, outcome = run_on_arbiter(test,testMethod,
                                                     istest=True)()
                    yield result
                    success = not self.add_failure(test, runner, outcome.result)
                    if success:
                        test.result = outcome.result
                    result, outcome = run_on_arbiter(test,test.tearDown)()
                    yield result
                    if self.add_failure(test, runner, outcome.result):
                        success = False
                else:
                    success = False
                    
            if hasattr(test,'_post_teardown'):
                result, outcome = run_on_arbiter(test,test._post_teardown)()
                yield result
                if self.add_failure(test, runner, outcome.result):
                    success = False
        
        except StopIteration:
            success = False
        except Exception as e:
            self.add_failure(test, runner, e)
            success = False
        
        if success:
            runner.addSuccess(test)
        

    def add_failure(self, test, runner, failure):
        failure = as_failure(failure)
        if failure:
            for trace in failure:
                e = trace[1]
                try:
                    raise e
                except test.failureException:
                    runner.addFailure(test, trace)
                except:
                    runner.addError(test, trace)
            return True
        else:
            return False