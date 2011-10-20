import os
import sys
import unittest
import logging
import inspect
from fnmatch import fnmatch

from pulsar.utils.importer import import_module


if not hasattr(unittest,'SkipTest'):
    class SkipTest(Exception):
        pass
else:
    SkipTest = unittest.SkipTest
    
    
default_logger = logging.getLogger('pulsar.apps.test.loader')
    
    
__all__ = ['TestLoader','SkipTest']
    

class TestLoader(object):
    '''Aggregate tests from a list of paths. The way it works is simple,
you give a *root* directory and a list of submodules where to look for tests.

:parameter root: root path.
:parameter modules: list (or tuple) of modules where to look for tests.
    A module can be a string indicating the **dotted** path relative to the
    **root** directory or a two element tuple with the same dotted path as
    first argument and a **pattern** which files must match in order to be
    included in the search.
    For example::
    
        modules = ['test']
        
    Lodas all tests from the ``test`` directory.
    All top level modules will be added to the python ``path``.
'''
    test_mapping = {'regression':'tests',
                    'benchmark':'benchmarks',
                    'profile':'profile'}
    
    def __init__(self, root, modules, test_type = 'regression', logger = None):
        self.log = logger or default_logger
        self.root = root
        self.test_type  = test_type
        if test_type not in self.test_mapping:
            raise ValueError('Test type {0} not available'.format(test_type)) 
        self.modules = modules
    
    def __repr__(self):
        return self.root
    __str__ = __repr__
        
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
            names = name.split('.')
            absolute_path = os.path.join(self.root,*names)
            if os.path.isdir(absolute_path):
                snames = names[:-1]
                if snames:
                    ppath = os.path.join(self.root,*snames)
                    if not ppath in sys.path:
                        sys.path.insert(0, ppath)
                    name = names[-1]
                for tag,mod in self.get_tests(absolute_path,name,pattern):
                    yield tag,mod
            else:
                raise ValueError('{0} cannot be found in {1} directory.'\
                                 .format(name,self.root))
                
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
                    except ImportError:
                        self.log.error('failed to import module {0}.\
 Skipping.'.format(subname), exc_info = True)
                    except:
                        self.log.critical('Failed to import module {0}.\
 Skipping.'.format(subname), exc_info = True)    
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
                    
        
        
    