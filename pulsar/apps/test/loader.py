import os
import re
import sys
import unittest
import logging

from pulsar.utils.importer import import_module

default_logger = logging.getLogger('pulsar.apps.test')


__all__ = ['TestLoader']


def issubclass_safe(cls, base_cls):
    try:
        return issubclass(cls, base_cls)
    except TypeError:
        return False

class TestLoader(object):
    '''Classes used by the :class:`TestSuite` to aggregate tests
from a list of paths. The way it works is simple,
you give a *root* directory and a list of submodules where to look for tests.

:parameter root: root path passed by the :class:`TestSuite`. 
:parameter modules: list (or tuple) of entries where to look for tests.
    An entry can be a string indicating the **dotted** path relative to the
    **root** directory or a two element tuple with the same dotted path as
    first argument and a **pattern** which files must match in order to be
    included in the search. A third element in the tuple can be used to specify
    the top level tag for all tests in this entry.
    For example::

        modules = ['test', ('bla', None, 'foo'), ('examples','test')]

    loads
    
     * all tests from modules in the ``test`` directory,
     * all tests from the ``bla`` directory with top level tag ``foo``,
     * all tests from the ``examples`` directory matching the test pattern.
     
    All top level modules will be added to the python ``path``.

:parameter runner: Instance of the test runner.

The are very :ref:`simple rules <apps-test-loading>` followed for
importing tests.
'''
    def __init__(self, root, modules, runner, logger=None):
        self.runner = runner
        self.logger = logger or default_logger
        self.root = root
        self.modules = []
        for mod in modules:
            if isinstance(mod, str):
                mod = (mod, None, None)
            if len(mod) < 3:
                mod = tuple(mod) + (None,) * (3 - len(mod))
            self.modules.append(mod)

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

    def testclasses(self, tags=None):
        pt = ', '.join(tags) if tags else 'all'
        self.logger.info('Load test classes for %s tags', pt)
        for tag, mod in self.testmodules(tags):
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
                if issubclass_safe(obj, unittest.TestCase):
                    yield tag, obj
    
    def testmodules(self, tags=None):
        '''Generator of tag, test modules pairs.'''
        for name, pattern, tag in self.modules:
            if pattern and '*' in pattern:
                pattern = re.compile(pattern.replace('*', '(.*)'))
            names = name.split('.') if name else ()
            absolute_path = os.path.join(self.root, *names)
            if os.path.isdir(absolute_path):
                pathbase = os.path.dirname(absolute_path)
                if pathbase not in sys.path:
                    sys.path.append(pathbase)
                name = names[-1]
                stags = (tag,) if tag else ()
                for tag, mod in self.get_tests(absolute_path, name, pattern,
                                               import_tags=tags, tags=stags):
                    yield tag, mod
            elif os.path.isfile(absolute_path + '.py'):
                tag = tag or name
                mod = self.import_module(name)
                if mod:
                    yield tag, mod
            else:
                raise ValueError('%s cannot be found in %s directory.'\
                                  % (name, self.root))

    def get_tests(self, path, dotted_path, pattern, import_tags=None,
                  tags=(), parent=None):
        '''Collect python modules for testing and return a generator of
tag,module pairs.

:parameter path: directory path where to search. Files starting with ``_``
    or ``.`` are excluded from the search, as well as non-python files.

:parameter dotted_path: the dotted python path equivalent of ``path``.

:parameter parent: the parent module for the current one. This parameter
    is passed by this function recursively.'''
        for mod_name in os.listdir(path):
            if mod_name.startswith('_') or mod_name.startswith('.'):
                continue
            mod_path = os.path.join(path, mod_name)
            if os.path.isfile(mod_path):
                if mod_name.endswith('.py'):
                    mod_name = mod_name.split('.')[0]
                else:
                    continue
            addtag = mod_name
            npattern = pattern
            if pattern:
                if hasattr(pattern, 'search'):
                    p = pattern.search(mod_name)
                    if p:
                        npattern = None
                        addtag = p.groups(0)
                    else:
                        addtag = False
                elif pattern == mod_name:
                    addtag = False
                    npattern = None
                if npattern and os.path.isfile(mod_path):
                    # skip the import
                    continue
            if dotted_path:
                mod_dotted_path = '%s.%s' % (dotted_path, mod_name)
            else:
                tags = (mod_name,)
                mod_dotted_path = mod_name
            module = self.import_module(mod_dotted_path, mod_path, parent)
            if not module:
                continue
            ctags = tags + (addtag,) if addtag else tags
            tag = '.'.join(ctags)
            c = self.checktag(tag, import_tags)
            if not c:
                continue
            counter = 0
            if os.path.isdir(mod_path):
                counter = 0
                # Recursively import modules
                for tag, mod in self.get_tests(mod_path,
                                               mod_dotted_path,
                                               npattern,
                                               import_tags,
                                               ctags,
                                               parent = module):
                    counter += 1
                    yield tag, mod
            # No submodules
            if not counter and c == 2:
                yield tag, module

    def import_module(self, name, path=None, parent=None):
        imp = True
        if path and os.path.isdir(path):
            imp = False
            # import only if it has a __init__.py file
            for sname in os.listdir(path):
                if sname == '__init__.py':
                    imp = True
                    break
        if imp:
            try:
                mod = import_module(name)
                if getattr(mod, '__test__', True):
                    return self.runner.import_module(mod, parent)
            except ImportError:
               self.logger.error('failed to import module %s. Skipping.',
                                 name, exc_info=True)
            except:
               self.logger.critical('Failed to import module %s. Skipping.',
                                    name, exc_info=True)

