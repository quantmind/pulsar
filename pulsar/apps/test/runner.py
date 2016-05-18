import asyncio
from inspect import isgenerator
from unittest import SkipTest, TestCase

from pulsar import ensure_future, isawaitable, HaltServer

from .utils import (TestFailure, skip_test, skip_reason,
                    expecting_failure, AsyncAssert, get_test_timeout)


class AbortTests(Exception):
    pass


class InvalidTestFunction(TestCase.failureException):
    pass


class Runner:

    def __init__(self, monitor, suite):
        cfg = suite.cfg
        loader = suite.loader
        tests = loader.test_files(cfg.labels, cfg.exclude_labels)
        #
        self._loop = monitor._loop
        self.cfg = suite.cfg
        self._time_start = self._loop.time()
        self.logger = monitor.logger
        self.suite = suite
        self.monitor = monitor
        self.loader = loader
        self.runner = loader.runner
        self.tests = list(reversed(tests))
        #
        self.runner.on_start()
        self._loop.call_soon(self._next_file)

    def _exit(self, exit_code):
        raise HaltServer(exit_code=exit_code)

    def _check_abort(self):
        if getattr(self._loop, 'exit_code', None):
            print('\nAbort tests')
            raise AbortTests

    def _next_file(self):
        if self.tests:
            tag, file_path = self.tests.pop()
            test_classes = iter(self.loader.import_module(file_path))
            self._loop.call_soon(self._next_class, tag, test_classes)

        else:
            time_taken = self._loop.time() - self._time_start
            runner = self.runner
            runner.on_end()
            runner.printSummary(time_taken)
            if runner.result.errors or runner.result.failures:
                exit_code = 2
            else:
                exit_code = 0
            self._loop.call_soon(self._exit, exit_code)

    def _next_class(self, tag, test_classes):
        try:
            test_cls = next(test_classes)
        except StopIteration:
            return self._loop.call_soon(self._next_file)

        test_cls.tag = tag
        test_cls.cfg = self.cfg
        test_cls.wait = AsyncAssert(test_cls)
        try:
            all_tests = self.runner.loadTestsFromTestCase(test_cls)
        except Exception:
            self.logger.exception('Could not load tests')
            run = False
        else:
            run = all_tests.countTestCases()

        if run:
            self.logger.info('Running Tests from %s', test_cls)
            self.runner.startTestClass(test_cls)
            coro = self._run_test_cls(test_cls, test_classes, all_tests)
            ensure_future(coro, loop=self._loop)
        else:
            self._loop.call_soon(self._next_class, tag, test_classes)

    async def _run_test_cls(self, test_cls, test_classes, all_tests):
        cfg = test_cls.cfg
        seq = getattr(test_cls, '_sequential_execution', cfg.sequential)
        test_timeout = get_test_timeout(test_cls, cfg.test_timeout)
        try:
            if skip_test(test_cls):
                raise SkipTest(skip_reason(test_cls))
            await self._run(test_cls.setUpClass, test_timeout)
        except SkipTest as exc:
            reason = str(exc)
            for test in all_tests:
                self.runner.addSkip(test, reason)
        except AbortTests:
            return
        except Exception as exc:
            self.logger.exception('Failure in setUpClass', exc_info=True)
            exc = TestFailure(exc)
            # setUpClass failed, fails all tests
            for test in all_tests:
                self.runner.startTest(test)
                self.add_failure(test, exc)
                self.runner.stopTest(test)
        else:
            try:
                if seq:
                    for test in all_tests:
                        await self._run_test(test, test_timeout)
                else:
                    await asyncio.wait([self._run_test(test, test_timeout)
                                        for test in all_tests],
                                       loop=self._loop)
            except AbortTests:
                return

        try:
            await self._run(test_cls.tearDownClass, test_timeout)
        except AbortTests:
            return
        except Exception:
            self.logger.exception('Failure in tearDownClass')

        self.logger.info('Finished Tests from %s', test_cls)
        self._loop.call_soon(self._next_class, test_cls.tag, test_classes)

    async def _run(self, method, test_timeout):
        self._check_abort()
        coro = method()
        # a coroutine
        if isawaitable(coro):
            test_timeout = get_test_timeout(method, test_timeout)
            await asyncio.wait_for(coro, test_timeout, loop=self._loop)
        elif isgenerator(coro):
            raise InvalidTestFunction('test function returns a generator')

    async def _run_test(self, test, test_timeout):
        '''Run a ``test`` function using the following algorithm

        * Run :meth:`setUp` method in :attr:`testcls`
        * Run the test function
        * Run :meth:`tearDown` method in :attr:`testcls`
        '''
        error = None
        runner = self.runner
        runner.startTest(test)
        test_name = test._testMethodName
        method = getattr(test, test_name)
        if skip_test(method):
            reason = skip_reason(method)
            runner.addSkip(test, reason)
        else:
            error = await self._run_safe(test, 'setUp', test_timeout)
            if not error:
                test = runner.before_test_function_run(test)
                error = await self._run_safe(test, test_name, test_timeout)
                runner.after_test_function_run(test)
            error = await self._run_safe(test, 'tearDown', test_timeout, error)
            if not error:
                runner.addSuccess(test)
        runner.stopTest(test)

    async def _run_safe(self, test, method_name, test_timeout, error=None):
        self._check_abort()
        try:
            method = getattr(test, method_name)
            coro = method()
            # a coroutine
            if isawaitable(coro):
                test_timeout = get_test_timeout(method, test_timeout)
                await asyncio.wait_for(coro, test_timeout, loop=self._loop)
            elif isgenerator(coro):
                raise InvalidTestFunction('test function returns a generator')
        except SkipTest as exc:
            self.runner.addSkip(test, str(exc))
        except Exception as exc:
            if not error:
                error = TestFailure(exc)
                self.add_failure(test, error, expecting_failure(method))
        return error

    def add_failure(self, test, failure, expecting_failure=False):
        '''Add ``error`` to the list of errors.

        :param test: the test function object where the error occurs
        :param runner: the test runner
        :param error: the python exception for the error
        :param add_err: if ``True`` the error is added to the list of errors
        :return: a tuple containing the ``error`` and the ``exc_info``
        '''
        runner = self.runner
        if expecting_failure:
            runner.addExpectedFailure(test, failure)
        elif isinstance(failure.exc, test.failureException):
            runner.addFailure(test, failure)
        else:
            runner.addError(test, failure)
