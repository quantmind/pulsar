import os
import unittest
import inspect

from pulsar.utils.importer import import_module


if not hasattr(unittest,'SkipTest'):
    class SkipTest(Exception):
        pass
else:
    SkipTest = unittest.SkipTest
    
    
__all__ = ['TestLoader','SkipTest']
    

class TestLoader(object):
    '''Aggregate tests from a list of paths. ''' 
    test_mapping = {'regression':'tests',
                    'benchmark':'benchmarks',
                    'profile':'profile'}
    def __init__(self, root, modules, test_type = 'regression'):
        self.root = root
        self.test_type  = test_type
        if test_type not in self.test_mapping:
            raise ValueError('Test type {0} not available'.format(test_type)) 
        self.modules = modules
    
    @property
    def testname(self):
        return self.test_mapping[self.test_type]
    
    def testclasses(self, tags = None):
        for tag,mod in self.testmodules():
            if tags and tag not in tags:
                continue
            for name in dir(mod):
                obj = getattr(mod, name)
                if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
                    yield tag,obj
            
    def testmodules(self):
        for name in self.modules:
            absolute_path = os.path.join(self.root,name)
            if os.path.isdir(absolute_path):
                for tag,mod in self.get_tests(absolute_path,name):
                    yield tag,mod
                
    def get_tests(self, path, name, tags = ()):
        testname = self.testname
        for sname in os.listdir(path):
            if sname.startswith('_'):
                continue
            subpath = os.path.join(path,sname)
            
            if os.path.isfile(subpath):
                if sname.endswith('.py'):
                    sname = sname.split('.')[0]
                else:
                    continue
            
            subname = '{0}.{1}'.format(name,sname)
            
            if sname == testname and tags:
                tag = '.'.join(tags)
                try:
                    module = import_module(subname)
                except:
                    print('failed to import module {0}. Skipping.'\
                          .format(subname))
                else:
                    yield tag, module
            elif os.path.isdir(subpath):
                for tag,mod in self.get_tests(subpath, subname, tags+(sname,)):
                    yield tag,mod
                

def TestVerbosity(level):
    if level is None:
        return 1
    else:
        return 2 if level > logging.DEBUG else 3


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

