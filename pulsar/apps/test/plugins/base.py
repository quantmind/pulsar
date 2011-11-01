import unittest

from pulsar.apps.test.config import TestOption
from pulsar.apps.test.result import TestObject


__all__ = ['WrapTest',
           'Plugin',
           'TestOptionPlugin']


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
        
    
class Plugin(TestObject):
    '''Base class for pulsar :class:`Application` plugins'''
    settings = ()
    
    
class TestOptionPlugin(Plugin,TestOption):
    '''Base class for test plugins with one option argument.'''
    virtual = True