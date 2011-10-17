import os
import unittest
import inspect
from fnmatch import fnmatch

from pulsar.utils.importer import import_module


if not hasattr(unittest,'SkipTest'):
    class SkipTest(Exception):
        pass
else:
    SkipTest = unittest.SkipTest
    
    
__all__ = ['TestLoader','SkipTest']
    

class TestLoader(object):
    '''Aggregate tests from a list of paths. The way it works is simple,
you give a *root* directory and a list of submodules where to look for tests.

:parameter root: root path.
:parameter modules: list of modules where to look for tests.
''' 
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
        for m in self.modules:
            if isinstance(m,str):
                name = m
                pattern = None
            else:
                name = m[0]
                pattern = m[1]
            absolute_path = os.path.join(self.root,name)
            if os.path.isdir(absolute_path):
                for tag,mod in self.get_tests(absolute_path,name,pattern):
                    yield tag,mod
                
    def get_tests(self, path, name, pattern, tags = ()):
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
            
            if pattern:
                if fnmatch(sname, pattern):
                    tag = '.'.join(tags)
                    try:
                        module = import_module(subname)
                    except:
                        print('failed to import module {0}. Skipping.'\
                              .format(subname))
                    else:
                        yield tag, module
                elif os.path.isdir(subpath):
                    for tag,mod in self.get_tests(subpath, subname, pattern,
                                                  tags+(sname,)):
                        yield tag,mod
            else:
                if os.path.isfile(subpath):
                    tag = '.'.join(tags+(sname,))
                    try:
                        module = import_module(subname)
                    except:
                        print('failed to import module {0}. Skipping.'\
                              .format(subname))
                    else:
                        yield tag, module
                elif os.path.isdir(subpath):
                    for tag,mod in self.get_tests(subpath, subname, pattern,
                                                  tags+(sname,)):
                        yield tag,mod
                    
        
        
    