import sys

from pulsar import multi_async, async
from pulsar.utils.pep import ispy26, ispy33
from pulsar.apps import tasks


if ispy26: # pragma nocover
    try:
        import unittest2 as unittest
        from unittest2.case import _ExpectedFailure as ExpectedFailure
    except ImportError:
        unittest = None
        ExpectedFailure = None
else:
    import unittest
    from unittest.case import _ExpectedFailure as ExpectedFailure
    
if ispy33:
    from unittest import mock
else: # pragma nocover
    try:
        import mock
    except ImportError:
        mock = None


class SetupError(Exception):
    pass

__all__ = ['sequential', 'unittest', 'mock']


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
            return self.run(runner, testcls, all_tests, consumer.worker.cfg)
        else:
            return runner.result
        
    @async()
    def run(self, runner, testcls, all_tests, cfg):
        '''Run all test functions from the :attr:`testcls` using the
following algorithm:

* Run the class method ``setUpClass`` of :attr:`testcls` if defined.
* Call :meth:`run_test` for each test functions in :attr:`testcls`
* Run the class method ``tearDownClass`` of :attr:`testcls` if defined.
'''
        timeout = cfg.test_timeout
        sequential = getattr(testcls, '_sequential_execution', cfg.sequential)
        skip_tests = getattr(testcls, "__unittest_skip__", False)
        test_cls = test_method(testcls, 'setUpClass')
        ok = yield self._run(runner, testcls, 'setUpClass', timeout, False)
        # run the tests
        if ok:
            if sequential:
                # Loop over all test cases in class
                for test in all_tests:
                    yield self.run_test(test, runner, cfg)
            else:
                all = (self.run_test(test, runner, cfg) for test in all_tests)
                yield multi_async(all)
        else:
            error = SetupError()
            for test in all_tests:
                self.add_failure(test, runner, error)
        yield self._run(runner, testcls, 'tearDownClass', timeout, False)
        yield runner.result
       
    def run_test(self, test, runner, cfg):
        '''Run a ``test`` function using the following algorithm

* Run :meth:`_pre_setup` method if available in :attr:`testcls`.
* Run :meth:`setUp` method in :attr:`testcls`.
* Run the test function.
* Run :meth:`tearDown` method in :attr:`testcls`.
* Run :meth:`_post_teardown` method if available in :attr:`testcls`.'''
        timeout = cfg.test_timeout
        ok = True
        try:
            runner.startTest(test)
            testMethod = getattr(test, test._testMethodName)
            if (getattr(test.__class__, "__unittest_skip__", False) or
                getattr(testMethod, "__unittest_skip__", False)):
                reason = (getattr(test.__class__,
                                  '__unittest_skip_why__', '') or
                          getattr(testMethod,
                                  '__unittest_skip_why__', ''))
                runner.addSkip(test, reason)
            else:
                ok = yield self._run(runner, test, '_pre_setup', timeout)
                if ok:
                    ok = yield self._run(runner, test, 'setUp', timeout)
                    if ok:
                        ok = yield self._run(runner, test, test._testMethodName,
                                             timeout)
                    ok = yield self._run(runner, test, 'tearDown',
                                         timeout, ok)
                ok = yield self._run(runner, test, '_post_teardown',
                                     timeout, ok)
                runner.stopTest(test)
        except Exception as e:
            ok = self.add_failure(test, runner, e, ok)
        else:
            if ok:
                runner.addSuccess(test)

    def _run(self, runner, test, method, timeout, ok=True):
        method = getattr(test, method, None)
        if method:
            try:
                yield runner.run_test_function(test, method, timeout)
                yield True
            except Exception as e:
                yield self.add_failure(test, runner, e, ok)
        else:
            yield True
        
    def add_failure(self, test, runner, error, ok=True):
        '''Add *failure* to the list of errors if *failure* is indeed a failure.
Return `True` if *failure* is a failure, otherwise return `False`.'''
        if ok:
            trace = sys.exc_info()
            if isinstance(error, test.failureException):
                runner.addFailure(test, trace)
            elif isinstance(error, ExpectedFailure):
                runner.addExpectedFailure(test, trace)
            else:
                runner.addError(test, trace)
        return False
        