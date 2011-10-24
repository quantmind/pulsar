import unittest

from pulsar.apps.test.config import TestOption


__all__ = ['WrapTest',
           'TestRunner',
           'Plugin',
           'TestOptionPlugin']


def _exc_info_to_string(self, err, test):
    exctype, value, tb = err
    if istraceback(tb):
        return self._original_exc_info_to_string(err,test)
    else:
        return ''.join(tb)
    
    
def monkey_patch(result):
    if not hasattr(result,'skipped'):
        result.skipped = []
    result._original_exc_info_to_string = result._exc_info_to_string
    result._exc_info_to_string = lambda a,b :\
                                     _exc_info_to_string(result,a,b)
    return results


class WrapTest(object):
    
    def __init__(self, test):
        self.test = test
        setattr(self, test._testMethodName, self._call)
        self.testMethod = getattr(test,test._testMethodName)

    def __str__(self):
        return self.test._testMethodName
    __repr__ = __str__
    
    def _call(self):
        return self.testMethod()
        

class TestRunner(object):
    
    def __init__(self, plugins, result = None):
        self.testsRun = 0
        self.cfg = None
        self.plugins = list(plugins)
        if result is None:
            r = unittest.TextTestRunner()
            result = r._makeResult()
        if result:
            stream = result.stream
            for p in self.plugins:
                p.stream = stream
            self.plugins.append(result)
        self.result = result
        
    def configure(self, cfg):
        self.cfg = cfg
        for p in self.plugins:
            if p.configure(cfg):
                break
        
    def getDescription(self, test):
        return test
        
    def import_module(self, mod, parent = None):
        for p in self.plugins:
            mod = p.import_module(mod,parent,self.cfg)
            if not mod:
                return
        return mod
    
    def startTest(self, test):
        '''Called before test has started.'''
        self.testsRun += 1
        for p in self.plugins:
            if p.startTest(test):
                break
            
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
            if p.addSuccess(test, err):
                break
            
    def addSkip(self, test, reason):
        for p in self.plugins:
            if p.addSkip(test, reason):
                break
    
    def printErrors(self):
        for p in self.plugins:
            if p.printErrors():
                break
            
    
class Plugin(object):
    '''Base class for pulsar :class:`Application` plugins'''
    settings = ()
    '''List or tuple of :attr:`Setting.name` used by the plugin.'''
    stream = None
    
    def configure(self, cfg):
        pass
    
    def setup(self, test, cfg):
        pass
    
    def import_module(self, mod, parent, cfg):
        '''Check if ``mod`` needs to be imported. If so it returns it.
        
:parameter mod: module to import
:parameter parent: parent module or ``None``.
:parameter cfg: :class:`pulsar.Config` instance.
:return: *mod* or ``None``. If ``None`` the module will be skipped.'''
        return mod
    
    def startTest(self, test):
        pass
    
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
    
    
class TestOptionPlugin(Plugin,TestOption):
    virtual = True