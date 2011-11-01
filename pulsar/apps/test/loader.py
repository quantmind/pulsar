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

:parameter runner: Instance of the test runner.
    
The are very :ref:`simple rules <apps-test-loading>` followed for
importing tests.
'''
    def __init__(self, root, modules, runner, logger = None):
        self.runner = runner
        self.log = logger or default_logger
        self.root = root
        self.modules = modules
    
    def __repr__(self):
        return self.root
    __str__ = __repr__
        
    def alltags(self, tag):
        bits = tag.split('.')
        tag,rest = bits[0],bits[1:]
        yield tag
        for b in rest:
            tag += '.' + b
            yield tag
    
    def checktag(self, tag, import_tags):
        '''return ``True`` if ``tag`` is in ``import_tags``.'''
        if import_tags:
            c = 0
            alltags = list(self.alltags(tag))
            for import_tag in import_tags:
                allitags = list(self.alltags(import_tag))
                for bit in alltags:
                    if bit == import_tag:
                        return 2
                    elif bit in allitags:
                        c = 1
            return c
        else:
            return 2
    
    def testclasses(self, tags = None):
        for tag,mod in self.testmodules(tags):
            if tags:
                skip = True
                for bit in self.alltags(tag):
                    if bit in tags:
                        skip = False
                        break
                if skip:
                    continue
            for name in dir(mod):
                obj = getattr(mod, name)
                if inspect.isclass(obj) and issubclass(obj, unittest.TestCase):
                    yield tag,obj
            
    def testmodules(self, tags = None):
        '''Generator of tag, test modules pair'''
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
                for tag,mod in self.get_tests(absolute_path,name,pattern,
                                              import_tags = tags):
                    yield tag,mod
            else:
                raise ValueError('{0} cannot be found in {1} directory.'\
                                 .format(name,self.root))
                
    def get_tests(self, path, name, pattern, import_tags = None,
                  tags = (), parent = None):
        for sname in os.listdir(path):
            if sname.startswith('_') or sname.startswith('.'):
                continue
            subpath = os.path.join(path,sname)
            
            if os.path.isfile(subpath):
                if sname.endswith('.py'):
                    sname = sname.split('.')[0]
                else:
                    continue
            
            addtag = True
            if pattern:
                if fnmatch(sname, pattern):
                    addtag = False
                elif os.path.isfile(subpath):
                    # skip the import
                    continue
                    
            subname = '{0}.{1}'.format(name,sname)            
            module = self.import_module(subname,parent)
            if not module:
                continue
            
            ctags = tags + (sname,) if addtag else tags
            tag = '.'.join(ctags)
            
            c = self.checktag(tag, import_tags)
            if not c:
                continue
            
            if os.path.isfile(subpath) and c == 2:
                tag = '.'.join(ctags)
                yield tag, module
            elif os.path.isdir(subpath):
                for tag,mod in self.get_tests(subpath, subname, pattern,
                                              import_tags, ctags,
                                              parent = module):
                    yield tag,mod
        
    def import_module(self, name, parent = None):
        try:
            mod = import_module(name)
            if getattr(mod,'__test__',True):
                return self.runner.import_module(mod,parent)
        except ImportError:
           self.log.error('failed to import module {0}. Skipping.'
                          .format(name), exc_info = True)
        except:
           self.log.critical('Failed to import module {0}. Skipping.'
                             .format(name), exc_info = True)    
        
    