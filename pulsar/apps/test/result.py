import unittest
from copy import deepcopy

from pulsar.utils.structures import AttributeDictionary

from .utils import TestFailure, LOGGER


__all__ = ['Plugin',
           'TestStream',
           'TestRunner',
           'TestResult']


STDOUT_LINE = '\nStdout:\n%s'
STDERR_LINE = '\nStderr:\n%s'


def def_return_val(c):
    return None


class Plugin:
    '''Interface for all classes which are part of the :class:`.TestRunner`.

    Most classes used by the test application are plugins, for
    example the :class:`.TestRunner` itself,
    the :class:`.TestResult` and the :class:`.TestPlugin`.
    '''
    result = None
    '''An optional result'''
    stream = None
    '''handle for writing text on the default output.

    Set by the :class:`.TestRunner` at runtime.
    '''
    descriptions = None

    def configure(self, cfg):
        '''Called once just after construction of a :class:`.TestRunner`
           and **before any test class is loaded**.

        This is a chance to configure the :class:`.Plugin` or global variables
        which may affect the way tests are run.
        If it returns something other than ``None`` (for example an abort
        message) it will stop the configuration of all subsequent
        plugins and quit the test.

        :parameter cfg: a :class:`.Config`.
        :return: ``None`` unless the tests runner must be stopped.
        '''
        pass

    @property
    def name(self):
        return self.__class__.__name__.lower()

    @property
    def count(self):
        return self.result.count if self.result else 0

    @property
    def testsRun(self):
        return self.result.testsRun if self.result else 0

    def on_start(self):
        '''Called by the :class:`.TestSuite` once only at startup.

        This callback is invoked once all tests are loaded but before
        the test suite starts running them.
        '''
        pass

    def on_end(self):
        '''Called by the :class:`.TestSuite` just before it stops.
        '''
        pass

    def loadTestsFromTestCase(self, testcls):
        '''Called when loading tests from the ``testcls`` class.

        Can be used to modify the number of test functions loaded.'''
        pass

    def startTestClass(self, testcls):
        '''Called just before a ``testcls`` runs its tests.
        '''
        pass

    def stopTestClass(self, testcls):
        '''Called just after a ``testcls`` has run its tests.
        '''
        pass

    def startTest(self, test):
        '''Called just before a ``test`` function is executed.

        This is run just before ``_pre_setup`` method.
        '''
        pass

    def stopTest(self, test):
        '''Called just after a ``test`` function has finished.

        This is run just after the ``_post_teardown`` method.
        '''
        pass

    def before_test_function_run(self, test, local):
        '''Can be used by plugins to manipulate the ``test``
        behaviour in the process domain where the test run.'''
        return test

    def after_test_function_run(self, test, local):
        '''Executed in the ``test`` process domain, after the ``test`` has
        finished.'''
        pass

    def addSuccess(self, test):
        '''Called when a ``test`` function succeed
        '''
        pass

    def addFailure(self, test, err):
        '''Called when a ``test`` function as a (test) failure
        '''
        pass

    def addError(self, test, err):
        '''Called when a ``test`` function as an (unexpected) error
        '''
        pass

    def addExpectedFailure(self, test, err):
        pass

    def addSkip(self, test, reason):
        pass

    def printErrors(self):
        pass

    def printSummary(self, timeTaken):
        pass

    def import_module(self, mod):
        return mod

    def getDescription(self, test):
        doc_first_line = test.shortDescription()
        teststr = '%s.%s' % (test.tag, test)
        if self.descriptions and doc_first_line:
            return '\n'.join((teststr, doc_first_line))
        else:
            return teststr


class TestStream(Plugin):  # pragma    nocover
    '''Handle the writing of test results'''
    separator1 = '=' * 70
    separator2 = '-' * 70

    def __init__(self, stream, result, descriptions=True):
        self._handlers = {}
        self.stream = stream
        self.result = result
        self.descriptions = descriptions
        self.showAll = False
        self.dots = True

    def configure(self, cfg):
        verbosity = cfg.verbosity
        self.showAll = verbosity > 1
        self.dots = verbosity == 1

    def handler(self, name):
        return self._handlers.get(name, self.stream)

    def startTest(self, test):
        if self.showAll:
            self.head(test, 'Started')

    def head(self, test, v):
        v = self.getDescription(test) + ' ... %s\n' % v
        self.stream.write(v)
        self.stream.flush()

    def addSuccess(self, test):
        if self.showAll:
            self.head(test, 'OK')
        elif self.dots:
            self.stream.write('.')
            self.stream.flush()

    def addError(self, test, err):
        if self.showAll:
            self.head(test, 'ERROR')
        elif self.dots:
            self.stream.write('E')
            self.stream.flush()

    def addFailure(self, test, err):
        if self.showAll:
            self.head(test, "FAIL")
        elif self.dots:
            self.stream.write('F')
            self.stream.flush()

    def addSkip(self, test, reason):
        if self.showAll:
            self.head(test, "skipped {0!r}".format(reason))
        elif self.dots:
            self.stream.write("s")
            self.stream.flush()

    def addExpectedFailure(self, test, err):
        if self.showAll:
            self.head(test, "expected failure")
        elif self.dots:
            self.stream.write("x")
            self.stream.flush()

    def addUnexpectedSuccess(self, test):
        if self.showAll:
            self.head(test, "unexpected success")
        elif self.dots:
            self.stream.write("u")
            self.stream.flush()

    def printErrors(self):
        if self.dots or self.showAll:
            self.stream.writeln()
        self.printErrorList('ERROR', self.result.errors)
        self.printErrorList('FAIL', self.result.failures)
        return True

    def printErrorList(self, flavour, errors):
        for test, err in errors:
            self.stream.writeln(self.separator1)
            self.stream.writeln("%s: %s" % (flavour, test))
            self.stream.writeln(self.separator2)
            self.stream.writeln("%s" % err)

    def printSummary(self, timeTaken):
        '''Write the summuray of tests results.'''
        stream = self.stream
        result = self.result
        self.printErrors()
        run = result.testsRun
        stream.writeln("Ran %d test%s in %.3fs" %
                       (run, run != 1 and "s" or "", timeTaken))
        stream.writeln()

        expectedFails = unexpectedSuccesses = skipped = 0
        results = map(len, (result.expectedFailures,
                            result.unexpectedSuccesses,
                            result.skipped))
        expectedFails, unexpectedSuccesses, skipped = results

        infos = []
        if not result.wasSuccessful():
            stream.write("FAILED")
            failed, errored = map(len, (result.failures, result.errors))
            if failed:
                infos.append("failures=%d" % failed)
            if errored:
                infos.append("errors=%d" % errored)
        else:
            stream.write("OK")
        if skipped:
            infos.append("skipped=%d" % skipped)
        if expectedFails:
            infos.append("expected failures=%d" % expectedFails)
        if unexpectedSuccesses:
            infos.append("unexpected successes=%d" % unexpectedSuccesses)
        if infos:
            stream.writeln(" (%s)" % (", ".join(infos),))
        else:
            stream.write("\n")

        return True


class TestResult(Plugin):
    '''A :class:`.Plugin` for collecting results/failures for test runs.

    Each :class:`.Plugin` can access the :class:`.TestRunner` ``result``
    object via the :attr:`~Plugin.result` attribute.
    '''
    def __init__(self, descriptions=True):
        self.descriptions = descriptions
        self._testsRun = 0
        self._count = 0
        self.failures = []
        self.errors = []
        self.skipped = []
        self.expectedFailures = []
        self.unexpectedSuccesses = []

    @property
    def count(self):
        return self._count

    @property
    def testsRun(self):
        return self._testsRun

    @property
    def result(self):
        return self

    def startTest(self, test):
        '''Increase the test counter
        '''
        self._testsRun += 1

    def addError(self, test, err):    # pragma    nocover
        '''Called when an unexpected error has occurred.

        ``err`` is a tuple of values as returned by ``sys.exc_info()``
        '''
        self._add_error(test, err, self.errors)

    def addFailure(self, test, err):    # pragma    nocover
        '''Called when an test failure has occurred.

        ``err`` is a tuple of values as returned by ``sys.exc_info()``
        '''
        self._add_error(test, err, self.failures)

    def addSkip(self, test, reason):
        """Called when a test is skipped."""
        self.skipped.append((self.getDescription(test), reason))

    def addExpectedFailure(self, test, err):
        """Called when an expected failure/error occured."""
        self._add_error(test, err, self.expectedFailures)

    def addUnexpectedSuccess(self, test):
        """Called when a test was expected to fail, but succeed."""
        self.unexpectedSuccesses.append(self.getDescription(test))

    def _add_error(self, test, exc, container):
        if not isinstance(exc, TestFailure):
            exc = TestFailure(exc)
        test = self.getDescription(test)
        container.append((test, str(exc)))

    def wasSuccessful(self):
        "Tells whether or not this result was a success"
        return len(self.failures) == len(self.errors) == 0


def testsafe(name, return_val=None):
    if not return_val:
        return_val = def_return_val

    def _(self, *args):
        for p in self.plugins:
            try:
                c = getattr(p, name)(*args)
                if c is not None:
                    return return_val(c)
            except Exception:       # pragma    nocover
                LOGGER.exception('Unhadled error in %s.%s' % (p, name))
    return _


class TestRunner(Plugin):
    '''A :class:`.Plugin` for asynchronously running tests.
    '''
    def __init__(self, plugins, stream, writercls=None, descriptions=True):
        self.descriptions = descriptions
        self.plugins = []
        writercls = writercls or TestStream
        result = TestResult(descriptions=self.descriptions)
        stream = writercls(stream, result, descriptions=self.descriptions)
        for p in plugins:
            p = deepcopy(p)
            p.descriptions = self.descriptions
            p.result = result
            p.stream = stream
            self.plugins.append(p)
        self.plugins.append(result)
        self.plugins.append(stream)
        self.stream = stream
        self.result = result
        self.loader = unittest.TestLoader()

    configure = testsafe('configure', lambda c: c)
    on_start = testsafe('on_start')
    on_end = testsafe('on_end')
    startTestClass = testsafe('startTestClass')
    stopTestClass = testsafe('stopTestClass')
    startTest = testsafe('startTest')
    stopTest = testsafe('stopTest')
    addSuccess = testsafe('addSuccess')
    addFailure = testsafe('addFailure')
    addExpectedFailure = testsafe('addExpectedFailure')
    addError = testsafe('addError')
    addSkip = testsafe('addSkip')
    printErrors = testsafe('printErrors')
    printSummary = testsafe('printSummary')

    def loadTestsFromTestCase(self, test_cls):
        '''Load all ``test`` functions for the ``test_cls``
        '''
        c = testsafe('loadTestsFromTestCase', lambda v: v)(self, test_cls)
        if c is None:
            return self.loader.loadTestsFromTestCase(test_cls)
        else:
            return c

    def import_module(self, mod):
        for p in self.plugins:
            mod = p.import_module(mod)
            if not mod:
                return
        return mod

    def before_test_function_run(self, test):
        '''Called just before the test is run'''
        test.plugins = plugins = {}
        for p in self.plugins:
            local = AttributeDictionary()
            plugins[p.name] = local
            test = p.before_test_function_run(test, local) or test
        return test

    def after_test_function_run(self, test):
        '''Called just after the test has finished,
        in the test process domain.'''
        for p in self.plugins:
            local = test.plugins.get(p.name)
            if local is not None:
                p.after_test_function_run(test, local)
