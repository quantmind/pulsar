import sys
import traceback
import unittest
import logging
from inspect import istraceback
from copy import deepcopy


__all__ = ['TestObject',
           'TestStream',
           'TestRunner',
           'TestResult']


LOGGER = logging.getLogger('pulsar.apps.test')
STDOUT_LINE = '\nStdout:\n%s'
STDERR_LINE = '\nStderr:\n%s'


class TestObject(object):
    '''Interface for all classes which are part of of the :class:`TestRunner`,
including :class:`TestRunner` itself, :class:`TestResult`
and :class:`Plugin`.'''
    descriptions = None
    
    def configure(self, cfg):
        '''Configure the *instance*. This method is called once just after
construction of a :class:`TestRunner` and can be used to configure the
plugin. If it returns something other than ``None``
(for example an abort message)
it will stop the configuration of all subsequent plugins and quit the test.

:parameter cfg: instance of :class:`pulsar.Config`.
'''
        pass
    
    def loadTestsFromTestCase(self, cls):
        pass
    
    def on_start(self):
        '''Called once by :class:`TestSuite` just before it starts running tests.'''
        pass
    
    def on_end(self):
        '''Called once by :class:`TestSuite` just before it stops.'''
        pass
    
    def startTest(self, test):
        pass
    
    def getTest(self, test):
        '''Given a test instance return a, possibly, modified test
instance. This function can be used by plugins to modify the behaviour of test
cases. By default it returns *test*.'''
        return test
    
    def stopTest(self, test):
        pass
    
    def addSuccess(self, test):
        pass
    
    def addFailure(self, test, err):
        pass
    
    def addError(self, test, err):
        pass
    
    def addSkip(self, test, reason):
        pass
    
    def printErrors(self):
        pass
    
    def printSummary(self, timeTaken):
        pass
    
    def import_module(self, mod, parent = None):
        return mod
    
    def getDescription(self, test):
        doc_first_line = test.shortDescription()
        teststr = '{0}.{1}'.format(test.tag,test)
        if self.descriptions and doc_first_line:
            return '\n'.join((teststr, doc_first_line))
        else:
            return teststr
    
    
class TestResultProxy(TestObject):
    result = None
    stream = None
    
    @property
    def count(self):
        return self.result.count if self.result else 0
    
    @property
    def testsRun(self):
        return self.result.testsRun if self.result else 0
    

class TestStream(TestResultProxy):
    separator1 = '=' * 70
    separator2 = '-' * 70
    
    def __init__(self, stream, result, descriptions = True):
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
        return self._handlers.get(name,self.stream)
    
    def startTest(self, test):
        pass
    
    def head(self, test, v):
        if self.showAll:
            v = self.getDescription(test) + ' ... ' + v + '\n'
            self.stream.write(v)
            self.stream.flush()
            
    def addSuccess(self, test):
        if self.showAll:
            self.head(test,'ok')
        elif self.dots:
            self.stream.write('.')
            self.stream.flush()
            
    def addError(self, test, err):
        if self.showAll:
            self.head(test,"ERROR")
        elif self.dots:
            self.stream.write('E')
            self.stream.flush()

    def addFailure(self, test, err):
        if self.showAll:
            self.head(test,"FAIL")
        elif self.dots:
            self.stream.write('F')
            self.stream.flush()

    def addSkip(self, test, reason):
        if self.showAll:
            self.head(test,"skipped {0!r}".format(reason))
        elif self.dots:
            self.stream.write("s")
            self.stream.flush()

    def addExpectedFailure(self, test, err):
        if self.showAll:
            self.head(test,"expected failure")
        elif self.dots:
            self.stream.write("x")
            self.stream.flush()

    def addUnexpectedSuccess(self, test):
        if self.showAll:
            self.head(test,"unexpected success")
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
            self.stream.writeln("%s: %s" % (flavour,test))
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


class TestResult(TestObject):
    
    def __init__(self, descriptions = True):
        self.descriptions = descriptions 
        self.testsRun = 0
        self._count = 0
        self.failures = []
        self.errors = []
        self.skipped = []
        self.expectedFailures = []
        self.unexpectedSuccesses = []
    
    @property
    def count(self):
        return self._count
    
    def startTest(self, test):
        self.testsRun += 1
    
    def addError(self, test, err):
        """Called when an error has occurred. 'err' is a tuple of values as
        returned by sys.exc_info().
        """
        self._add_error(test, err, self.errors)
        
    def addFailure(self, test, err):
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
    
    def _add_error(self, test, err, container):
        err = self._exc_info_to_string(err, test)
        test = self.getDescription(test)
        container.append((test, err))
                
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
        
    def _exc_info_to_string(self, err, test):
        """Converts a sys.exc_info()-style tuple of values into a string."""
        exctype, value, tb = err
        # Skip test runner traceback levels
        if istraceback(tb):
            while tb and self._is_relevant_tb_level(tb):
                tb = tb.tb_next
    
            if exctype is test.failureException:
                # Skip assert*() traceback levels
                length = self._count_relevant_tb_levels(tb)
                msgLines = traceback.format_exception(exctype, value, tb, length)
            else:
                msgLines = traceback.format_exception(exctype, value, tb)
        else:
            msgLines = tb or []

        return ''.join(msgLines)


    def _is_relevant_tb_level(self, tb):
        return '__unittest' in tb.tb_frame.f_globals

    def _count_relevant_tb_levels(self, tb):
        length = 0
        while tb and not self._is_relevant_tb_level(tb):
            length += 1
            tb = tb.tb_next
        return length

    
class TestRunner(TestResultProxy):
    '''An asynchronous test runner'''
    def __init__(self, plugins, stream, writercls = None, descriptions=True):
        self.descriptions = descriptions
        self.plugins = []
        writercls = writercls or TestStream
        result = TestResult(descriptions = self.descriptions)
        stream = writercls(stream, result, descriptions = self.descriptions)
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
        
    def configure(self, cfg):
        self.cfg = cfg
        for p in self.plugins:
            c = p.configure(cfg)
            if c:
                return c 
    
    def loadTestsFromTestCase(self, cls):
        all_tests = self.loader.loadTestsFromTestCase(cls)
        for p in self.plugins:
            c = p.loadTestsFromTestCase(cls)
            if c is not None:
                return c
        return all_tests
        
    def on_start(self):
        '''Called just before the test suite starts running tests.'''
        for p in self.plugins:
            if p.on_start():
                break
            
    def on_end(self):
        '''Called just before the test suite starts running tests.'''
        for p in self.plugins:
            try:
                if p.on_end():
                    break
            except:
                LOGGER.critical('Unhandled exception while calling method'\
                                ' "on_end" of plugin {0}'.format(p),
                                exc_info=True)
        
    def add(self, result):
        self.result.add(result)
    
    def import_module(self, mod, parent = None):
        for p in self.plugins:
            mod = p.import_module(mod,parent)
            if not mod:
                return
        return mod
    
    def startTest(self, test):
        '''Called before test has started.'''
        for p in self.plugins:
            if p.startTest(test):
                break
            
    def getTest(self, test):
        for p in self.plugins:
            test = p.getTest(test) or test
        return test
            
    def stopTest(self, test):
        '''Called after test has finished.'''
        for p in self.plugins:
            if p.stopTest(test):
                break
            
    def addSuccess(self, test):
        for p in self.plugins:
            if p.addSuccess(test):
                break
            
    def addFailure(self, test, err):
        for p in self.plugins:
            if p.addFailure(test, err):
                break
            
    def addError(self, test, err):
        for p in self.plugins:
            if p.addError(test, err):
                break
            
    def addSkip(self, test, reason):
        for p in self.plugins:
            if p.addSkip(test, reason):
                break
    
    def printErrors(self):
        for p in self.plugins:
            if p.printErrors():
                break
    
    def printSummary(self, timeTaken):
        for p in self.plugins:
            if p.printSummary(timeTaken):
                break