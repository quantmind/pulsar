from pulsar import is_failure, multi_async, maybe_async, maybe_failure
from pulsar.apps import tasks


__all__ = ['sequential']


def sequential(cls):
    '''Decorator for a :class:`TestCase` which cause its test functions to run
sequentially rather than in an asynchronous fashion.'''
    cls._sequential_execution = True
    return cls

def test_method(cls, method):
    try:
        return cls(method)
    except ValueError:
        return None

    
class Test(tasks.Job):
    
    def __call__(self, consumer, testcls, tag):
        suite = consumer.worker.app
        suite.local.pop('runner')
        runner = suite.runner
        if not isinstance(testcls, type):
            testcls = testcls()
        testcls.tag = tag
        testcls.cfg = consumer.worker.cfg
        suite.logger.debug('Testing %s', testcls.__name__)
        all_tests = runner.loadTestsFromTestCase(testcls)
        num = all_tests.countTestCases()
        if num:
            result = self.run(runner, testcls, all_tests, consumer.worker.cfg)
            return maybe_async(result, max_errors=None)
        else:
            return runner.result
        
    def run(self, runner, testcls, all_tests, cfg):
        '''Run all test functions from the :attr:`testcls` using the
following algorithm:

* Run the class method ``setUpClass`` of :attr:`testcls` if defined.
* Call :meth:`run_test` for each test functions in :attr:`testcls`
* Run the class method ``tearDownClass`` of :attr:`testcls` if defined.
'''
        run_test_function = runner.run_test_function
        sequential = getattr(testcls, '_sequential_execution', cfg.sequential)
        skip_tests = getattr(testcls, "__unittest_skip__", False)
        should_stop = False
        test_cls = test_method(testcls, 'setUpClass')
        if test_cls and not skip_tests:
            outcome = yield run_test_function(testcls,
                                        getattr(testcls,'setUpClass'),
                                        cfg.test_timeout)
            should_stop = self.add_failure(test_cls, runner, outcome)
        #
        # run the tests
        if not should_stop:
            if sequential:
                # Loop over all test cases in class
                for test in all_tests:
                    yield self.run_test(test, runner, cfg)
            else:
                all = (self.run_test(test, runner, cfg) for test in all_tests)
                yield multi_async(all)
        #
        test_cls = test_method(testcls, 'tearDownClass')
        if test_cls and not skip_tests:
            outcome = yield run_test_function(testcls,
                                              getattr(testcls,'tearDownClass'),
                                              cfg.test_timeout)
            self.add_failure(test_cls, runner, outcome)
        yield runner.result
       
    def run_test(self, test, runner, cfg):
        '''\
Run a *test* function using the following algorithm

* Run :meth:`_pre_setup` method if available in :attr:`testcls`.
* Run :meth:`setUp` method in :attr:`testcls`.
* Run the test function.
* Run :meth:`tearDown` method in :attr:`testcls`.
* Run :meth:`_post_teardown` method if available in :attr:`testcls`.
'''
        return maybe_async(self._run_test(test, runner, cfg.test_timeout),
                           max_errors=None)
    
    def _run_test(self, test, runner, test_timeout):
        try:
            ok = True
            runner.startTest(test)
            run_test_function = runner.run_test_function
            testMethod = getattr(test, test._testMethodName)
            if (getattr(test.__class__, "__unittest_skip__", False) or
                getattr(testMethod, "__unittest_skip__", False)):
                reason = (getattr(test.__class__,
                                  '__unittest_skip_why__', '') or
                          getattr(testMethod,
                                  '__unittest_skip_why__', ''))
                runner.addSkip(test, reason)
                raise StopIteration
            # _pre_setup function if available
            if hasattr(test,'_pre_setup'):
                outcome = yield run_test_function(test, test._pre_setup,
                                                  test_timeout)
                ok = ok and not self.add_failure(test, runner, outcome)
            # _setup function if available
            if ok:
                outcome = yield run_test_function(test, test.setUp,
                                                  test_timeout)
                ok = not self.add_failure(test, runner, outcome)
                if ok:
                    # Here we perform the actual test
                    outcome = yield run_test_function(test, testMethod,
                                                      test_timeout)
                    ok = not self.add_failure(test, runner, outcome)
                    if ok:
                        test.result = outcome
                    outcome = yield run_test_function(test, test.tearDown,
                                                      test_timeout)
                    ok = ok and not self.add_failure(test, runner, outcome)
            # _post_teardown
            if hasattr(test,'_post_teardown'):
                outcome = yield run_test_function(test,test._post_teardown,
                                                  test_timeout)
                if ok:
                    ok = not self.add_failure(test, runner, outcome)
            # run the stopTest
            runner.stopTest(test)
        except StopIteration:
            pass
        except Exception as e:
            if ok:
                ok = not self.add_failure(test, runner, e)
        else:
            if ok:
                runner.addSuccess(test)

    def add_failure(self, test, runner, failure):
        '''Add *failure* to the list of errors if *failure* is indeed a failure.
Return `True` if *failure* is a failure, otherwise return `False`.'''
        failure = maybe_failure(failure)
        if is_failure(failure):
            if failure.isinstance(test.failureException):
                runner.addFailure(test, failure.trace)
            else:
                runner.addError(test, failure.trace)
            return True
        else:
            return False
        