'''\
Testing application. Pulsar tests uses exatly the same API as any
pulsar server. The Test suite is the Arbiter while the
Worker class runs the tests in an asychronous way.
'''
import unittest
import logging
import os
import sys
import time
import inspect

import pulsar
from pulsar.utils.importer import import_module


if not hasattr(unittest,'SkipTest'):
    class SkipTest(Exception):
        pass
else:
    SkipTest = unittest.SkipTest

def TestVerbosity(level):
    if level is None:
        return 1
    else:
        return 2 if level > logging.DEBUG else 3

class TestType(pulsar.Setting):
    name = "test_type"
    section = "Test"
    meta = "STRING"
    cli = ["--test-type"]
    validator = pulsar.validate_string
    default = 'regression'
    desc = """\
        The test type.
        Possible choices are: regression, bench and profile.
    """


class StreamLogger(object):
    
    def __init__(self, log):
        self.log = log
        self.msg = ''
        
    def write(self,msg):
        if msg == '\n':
            self.flush()
        else:
            self.msg += msg

    def flush(self):
        msg = self.msg
        self.msg = ''
        self.log.info(msg)


class TestCbk(object):
    
    def __call__(self, result):
        self.result = result
        
        
class TestGenerator(object):
    
    def __init__(self, test, result, testMethod):
        self.test = test
        self.failureException = test.failureException
        self.shortDescription = test.shortDescription
        self.result = result
        test.success = False
        self.testMethod = testMethod
        try:
            test.setUp()
            test.success = True
        except SkipTest as e:
            result.addSkip(self, str(e))
        except Exception:
            result.addError(self.test, sys.exc_info())
        
    def __call__(self):
        result = self.result
        test = self.test
        if test.success:
            try:
                test.success = False
                self.testMethod()
            except test.failureException:
                result.addFailure(test, sys.exc_info())
            except SkipTest as e:
                result.addSkip(self.test, str(e))
            except Exception:
                result.addError(self.test, sys.exc_info())
            else:
                test.success = True
        self.close()
    
    def close(self):
        result = self.result
        test = self.test
        try:
            try:
                test.tearDown()
            except Exception:
                result.addError(test, sys.exc_info())
                test.success = False
    
            if hasattr(test,'doCleanups'):
                cleanUpSuccess = test.doCleanups()
                test.success = test.success and cleanUpSuccess
                
            if test.success:
                result.addSuccess(test)
        finally:
            result.stopTest(self) 
        

class TestCase(unittest.TestCase):
    '''A specialised test case which offers three
additional functions:

a) 'initTest' and 'endTests', called at the beginning and at the end
of the tests declared in a derived class. Useful for starting a server
to send requests to during tests.

b) 'runInProcess' to run a callable in the main process.'''
    suiterunner = None
    
    def __init__(self, methodName=None):
        if methodName:
            self._dummy = False
            super(TestCase,self).__init__(methodName)
        else:
            self._dummy = True
    
    def __repr__(self):
        if self._dummy:
            return self.__class__.__name__
        else:
            return super(TestCase,self).__repr__()
    
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
    
    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()
            startTestRun = getattr(result, 'startTestRun', None)
            if startTestRun is not None:
                startTestRun()

        self._resultForDoCleanups = result
        result.startTest(self)
        if getattr(self.__class__, "__unittest_skip__", False):
            # If the whole class was skipped.
            try:
                result.addSkip(self, self.__class__.__unittest_skip_why__)
            finally:
                result.stopTest(self)
            return
        testMethod = getattr(self, self._testMethodName)
        TestGenerator(self, result, testMethod)()
        
    
class TestSuite(unittest.TestSuite):
    '''A test suite for the modified TestCase.'''
    loader = unittest.TestLoader()
    
    def addTest(self, test):
        tests = self.loader.loadTestsFromTestCase(test)
        if tests:
            try:
                obj = test()
            except:
                obj = test
            self._tests.append({'obj':obj,
                                'tests':tests})

        
class TestLoader(object):
    '''Load test cases'''
    suiteClass = TestSuite
    
    def __init__(self, tags, testtype, extractors, itags = None):
        self.tags = tags
        self.testtype = testtype
        self.extractors = extractors
        self.itags = itags
        
    def load(self, suiterunner):
        """Return a suite of all tests cases contained in the given module.
It injects the suiterunner proxy for comunication with the master process."""
        itags = self.itags or []
        tests = []
        for module in self.modules(suiterunner.log):
            for name in dir(module):
                obj = getattr(module, name)
                if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
                    tag = getattr(obj,'tag',None)
                    if tag and not tag in itags:
                        continue
                    obj.suiterunner = suiterunner
                    obj.log = suiterunner.log
                    tests.append(obj)
        return self.suiteClass(tests)
    
    def get_tests(self,dirpath):
        join  = os.path.join
        loc = os.path.split(dirpath)[1]
        for d in os.listdir(dirpath):
            if d.startswith('__'):
                continue
            if os.path.isdir(join(dirpath,d)):
                yield (loc,d)
            
    def modules(self, log):
        tags,testtype,extractors = self.tags,self.testtype,self.extractors
        for extractor in extractors:
            testdir = extractor.testdir(testtype)
            for loc,app in self.get_tests(testdir):
                if tags and app not in tags:
                    log.debug("Skipping tests for %s" % app)
                    continue
                log.debug("Try to import tests for %s" % app)
                test_module = extractor.test_module(testtype,loc,app)
                try:
                    mod = import_module(test_module)
                except ImportError as e:
                    log.debug("Could not import tests for %s: %s" % (test_module,e))
                    continue
                
                log.debug("Adding tests for %s" % app)
                yield mod
    
    
class TextTestRunner(unittest.TextTestRunner):
    
    def run(self, tests):
        "Run the given test case or test suite."
        result = self._makeResult()
        result.startTime = time.time()
        for test in tests:
            if result.shouldStop:
                raise StopIteration
            obj = test['obj']
            init = getattr(obj,'initTests',None)
            end = getattr(obj,'endTests',None)
            if init:
                try:
                    yield init()
                except Exception as e:
                    result.shouldStop = True
                    yield StopIteration
            for t in test['tests']:
                yield t(result)
            if end:
                try:
                    yield end()
                except Exception as e:
                    result.shouldStop = True
                    yield StopIteration
        yield self.end(result)
            
    def end(self, result):
        stopTestRun = getattr(result, 'stopTestRun', None)
        if stopTestRun is not None:
            stopTestRun()
        result.stopTime = time.time()
        timeTaken = result.stopTime - result.startTime
        result.printErrors()
        if hasattr(result, 'separator2'):
            self.stream.writeln(result.separator2)
        run = result.testsRun
        self.stream.writeln("Ran %d test%s in %.3fs" %
                            (run, run != 1 and "s" or "", timeTaken))
        self.stream.writeln()

        expectedFails = unexpectedSuccesses = skipped = 0
        try:
            results = map(len, (result.expectedFailures,
                                result.unexpectedSuccesses,
                                result.skipped))
        except AttributeError:
            pass
        else:
            expectedFails, unexpectedSuccesses, skipped = results

        infos = []
        if not result.wasSuccessful():
            self.stream.write("FAILED")
            failed, errored = len(result.failures), len(result.errors)
            if failed:
                infos.append("failures=%d" % failed)
            if errored:
                infos.append("errors=%d" % errored)
        else:
            self.stream.write("OK")
        if skipped:
            infos.append("skipped=%d" % skipped)
        if expectedFails:
            infos.append("expected failures=%d" % expectedFails)
        if unexpectedSuccesses:
            infos.append("unexpected successes=%d" % unexpectedSuccesses)
        if infos:
            self.stream.writeln(" (%s)" % (", ".join(infos),))
        else:
            self.stream.write("\n")
        return result


class TestMonitor(pulsar.WorkerMonitor):
    '''A specialized worker monitor for testing.'''
    def info(self):
        return self._info(len(self.LIVE_ACTORS))


class TestApplication(pulsar.Application):
    producer = None
    done = False
    default_logging_level = None
    monitor_class = TestMonitor
    cfg = {'timeout':300,
           'concurrency':'thread',
           'workers':1,
           'worker_class':'base',
           'loglevel':'none'}
    
    def init(self, parser = None, opts = None, args = None, extractors = None):
        self.tags = args
    
    def handler(self):
        return self
    
    def worker_task(self, worker):
        '''At each event loop we run a test'''
        if not self.done:
            if self.producer is None:
                try:
                    cfg = self.cfg
                    suite =  TestLoader(self.tags, cfg.test_type,
                                        self.extractors).load(worker)
                    verbosity = TestVerbosity(self.loglevel)
                    if self.loglevel is not None:
                        stream = StreamLogger(worker.log)
                        producer = TextTestRunner(stream = stream,
                                                  verbosity = verbosity)
                    else:
                        producer = TextTestRunner(verbosity = verbosity)
                    self.producer = producer.run(suite)
                    self.producers = []
                except Exception as e:
                    worker.log.critical('Could not start tests. {0}'.format(e),
                                        exc_info = sys.exc_info())
                    self.done = True
                    worker.shut_down()
                    return
            try:
                p = next(self.producer)
                if inspect.isgenerator(p):
                    self.producers.append(self.producer)
                    self.producer = p
            except StopIteration:
                if self.producers:
                    self.producer = self.producers.pop()
                else:
                    self.done = True
                    worker.shut_down()
                      
        
def TestSuiteRunner(extractors):
    return TestApplication(extractors = extractors)
    
