'''
.. autoclass:: TestLoader
   :members:
   :member-order: bysource

'''
import os
import re
import sys
import unittest
from importlib import import_module

from .utils import LOGGER


__all__ = ['TestLoader']


no_tags = ('tests', 'test')
test_patterns = [re.compile(r'''test_(?P<name>.*).py'''),
                 re.compile(r'''(?P<name>.*)_test.py'''),
                 re.compile(r'''tests.py''')]


def issubclass_safe(cls, base_cls):
    try:
        return issubclass(cls, base_cls)
    except TypeError:
        return False


class TestLoader:
    '''Classes used by the :class:`.TestSuite` to aggregate tests
    from a list of paths.

    The way it works is simple, you give a *root* directory and a list
    of submodules where to look for tests.

    :parameter root: root path passed by the :class:`.TestSuite`.
    :parameter modules: list (or tuple) of entries where to look for tests.
        Check :ref:`loading test documentation <apps-test-loading>` for
        more information.
    :parameter runner: The :class:`.TestRunner` passed by the test suite.
    '''
    def __init__(self, root, modules, runner, logger=None):
        self.runner = runner
        self.logger = logger or LOGGER
        self.root = root
        self.modules = modules

    def __repr__(self):
        return self.root
    __str__ = __repr__

    def alltags(self, tag):
        bits = tag.split('.')
        tag, rest = bits[0], bits[1:]
        yield tag
        for b in rest:
            tag += '.' + b
            yield tag

    def checktag(self, tag, import_tags, exclude_tags):
        '''Return ``True`` if ``tag`` is in ``import_tags``.'''
        if exclude_tags:
            alltags = list(self.alltags(tag))
            for exclude_tag in exclude_tags:
                for bit in alltags:
                    if bit == exclude_tag:
                        return 0
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

    def testclasses(self, tags=None, exclude_tags=None):
        pt = ', '.join(('"%s"' % t for t in tags)) if tags else 'all'
        ex = ((' excluding %s' % ', '.join(('"%s"' % t for t in exclude_tags)))
              if exclude_tags else '')
        self.logger.info('Load test classes for %s %s', pt, ex)
        for tag, mod in self.testmodules(tags, exclude_tags):
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

    def testmodules(self, tags=None, exclude_tags=None):
        """Generator of ``tag``, ``modules`` pairs.

        :parameter tags: optional list of tags to include, if not available
            all tags will be included.
        :parameter exclude_tags: optional list of tags to exclude.
            If not provided no tags will be excluded.
        """
        d = dict(self._testmodules(tags, exclude_tags))
        return [(k, d[k]) for k in sorted(d)]

    def _testmodules(self, include, exclude):
        if not self.modules:
            yield from self.get_tests(self.root, include, exclude)
        else:
            for name in self.modules:
                absolute_path = os.path.join(self.root, name)
                if os.path.isdir(absolute_path):
                    self.logger.debug('Loading from "%s"', name)
                    yield from self.get_tests(absolute_path, include, exclude)
                else:
                    raise ValueError('%s cannot be found in %s directory.'
                                     % (name, self.root))

    def get_tests(self, path, include_tags, exclude_tags, tags=None):
        """Collect python modules for testing.

            :parameter path: directory path where to search. Files starting
                with ``_`` or ``.`` are excluded from the search,
                as well as non-python files.
            :parameter dotted_path: the dotted python path equivalent
                of ``path``.
            :parameter parent: the parent module for the current one.
                This parameter is passed by this function recursively
            :return: a generator of tag, module pairs.
        """
        if tags is None:
            tags = []

        for mod_name in os.listdir(path):
            if mod_name.startswith('_') or mod_name.startswith('.'):
                continue
            mod_path = os.path.join(path, mod_name)
            is_file = os.path.isfile(mod_path)
            if is_file:
                tag = self.match(mod_name)
                if tag is None:  # does not match and is a file, skip.
                    continue
                m_tags = tags + list(tag)
            else:
                m_tags = tags if mod_name in no_tags else tags + [mod_name]

            tag = '.'.join(m_tags)
            c = self.checktag(tag, include_tags, exclude_tags)
            if not c:
                continue

            if is_file:
                module = self.import_module(mod_path)
                if not module:
                    continue
                yield tag, module

            else:
                yield from self.get_tests(mod_path, include_tags,
                                          exclude_tags, m_tags)

    def import_module(self, file_name):
        path, name = os.path.split(file_name)
        add_to_path = path not in sys.path
        try:
            if add_to_path:
                sys.path.insert(0, path)
            mod = import_module(name[:-3])
            if getattr(mod, '__test__', True):
                return self.runner.import_module(mod)
        except ImportError:
            self.logger.error('Failed to import module %s. Skipping.',
                              name, exc_info=True)
            self.logger.debug('Full python path:\n%s', '\n'.join(sys.path))
        except Exception:
            self.logger.critical('Failed to import module %s. Skipping.',
                                 name, exc_info=True)
        finally:
            if add_to_path:
                sys.path.pop(0)

    def match(self, name):
        for pattern in test_patterns:
            p = pattern.search(name)
            if p:
                return p.groups(0)
