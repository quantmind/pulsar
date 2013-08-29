import sys
import logging

from pulsar import multi_async, async
from pulsar.utils.pep import ispy26, ispy33
from pulsar.apps import tasks


LOGGER = logging.getLogger('pulsar.apps.test')


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


__all__ = ['sequential', 'unittest', 'mock', 'get_stream']


def get_stream(cfg):
    r = unittest.TextTestRunner()
    return r.stream

def sequential(cls):
    '''Decorator for a :class:`TestCase` which cause its test functions to run
sequentially rather than in an asynchronous fashion.'''
    cls._sequential_execution = True
    return cls

    
class Test(tasks.Job):
    '''A :ref:`Job <job-callable>` for running tests on a task queue.'''
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

* Run the class method ``setUpClass`` of :attr:`testcls` if defined, unless
  the test class should be skipped.
* Call :meth:`run_test` for each test functions in :attr:`testcls`
* Run the class method ``tearDownClass`` of :attr:`testcls` if defined, unless
  the test class should be skipped.'''
        error = None
        timeout = cfg.test_timeout
        sequential = getattr(testcls, '_sequential_execution', cfg.sequential)
        skip_tests = getattr(testcls, '__unittest_skip__', False)
        if not skip_tests:
            error = yield self._run(runner, testcls, 'setUpClass', timeout,
                                    add_err=False)
        # run the tests
        if not error:
            if sequential:
                # Loop over all test cases in class
                for test in all_tests:
                    yield self.run_test(test, runner, cfg)
            else:
                all = (self.run_test(test, runner, cfg) for test in all_tests)
                yield multi_async(all)
        else:
            for test in all_tests:
                runner.startTest(test)
                self.add_failure(test, runner, error[0], error[1])
                runner.stopTest(test)
        if not skip_tests:
            yield self._run(runner, testcls, 'tearDownClass', timeout,
                            add_err=False)
        yield runner.result
       
    def run_test(self, test, runner, cfg):
        '''Run a ``test`` function using the following algorithm

* Run :meth:`_pre_setup` method if available in :attr:`testcls`.
* Run :meth:`setUp` method in :attr:`testcls`.
* Run the test function.
* Run :meth:`tearDown` method in :attr:`testcls`.
* Run :meth:`_post_teardown` method if available in :attr:`testcls`.'''
        timeout = cfg.test_timeout
        err = None
        try:
            runner.startTest(test)
            testMethod = getattr(test, test._testMethodName)
            if (getattr(test.__class__, '__unittest_skip__', False) or
                getattr(testMethod, '__unittest_skip__', False)):
                reason = (getattr(test.__class__,
                                  '__unittest_skip_why__', '') or
                          getattr(testMethod,
                                  '__unittest_skip_why__', ''))
                runner.addSkip(test, reason)
                err = True
            else:
                err = yield self._run(runner, test, '_pre_setup', timeout)
                if not err:
                    err = yield self._run(runner, test, 'setUp', timeout)
                    if not err:
                        err = yield self._run(runner, test,
                                              test._testMethodName, timeout)
                    err = yield self._run(runner, test, 'tearDown',
                                          timeout, err)
                err = yield self._run(runner, test, '_post_teardown',
                                      timeout, err)
                runner.stopTest(test)
        except Exception as error:
            self.add_failure(test, runner, error, err)
        else:
            if not err:
                runner.addSuccess(test)

    def _run(self, runner, test, method, timeout, previous=None, add_err=True):
        method = getattr(test, method, None)
        if method:
            try:
                yield runner.run_test_function(test, method, timeout)
                yield previous
            except Exception as error:
                add_err = False if previous else add_err
                yield self.add_failure(test, runner, error, add_err=add_err)
        else:
            yield previous
        
    def add_failure(self, test, runner, error, trace=None, add_err=True):
        '''Add *failure* to the list of errors if *failure* is indeed a failure.
Return `True` if *failure* is a failure, otherwise return `False`.'''
        if not trace:
            trace = sys.exc_info()
        if add_err:
            if isinstance(error, test.failureException):
                runner.addFailure(test, trace)
            elif isinstance(error, ExpectedFailure):
                runner.addExpectedFailure(test, trace)
            else:
                runner.addError(test, trace)
        return (error, trace)
        