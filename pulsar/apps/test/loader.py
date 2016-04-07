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

import pulsar

from .result import TestRunner


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
    def __init__(self, suite):
        self.stream = pulsar.get_stream(suite.cfg)
        self.runner = TestRunner(suite.cfg.test_plugins, self.stream)
        self.abort_message = self.runner.configure(suite.cfg)
        self.root = suite.root_dir
        self.modules = suite.cfg.test_modules
        self.logger = suite.logger

    def tags(self, include=None, exclude=None):
        """Return a generator of tag information
        """
        for tag, file_path in self.test_files(include, exclude):
            with self.import_module(file_path) as importer:
                if importer.module:
                    doc = importer.module.__doc__
                    if doc:
                        tag = '{0} - {1}'.format(tag, doc)
                    yield tag

    def test_modules(self, tags):
        for tag, file_path in self.test_files(tags):
            mod = self.import_module(file_path)
            if mod:
                yield mod

    def test_files(self, tags=None, exclude_tags=None):
        """List of ``tag``, ``modules`` pairs.

        :parameter tags: optional list of tags to include, if not available
            all tags will be included.
        :parameter exclude_tags: optional list of tags to exclude.
            If not provided no tags will be excluded.
        """
        d = dict(self._test_files(tags, exclude_tags))
        return [(k, d[k]) for k in sorted(d)]

    # INTERNALS
    def _all_tags(self, tag):
        bits = tag.split('.')
        tag, rest = bits[0], bits[1:]
        yield tag
        for b in rest:
            tag += '.' + b
            yield tag

    def _check_tag(self, tag, import_tags, exclude_tags):
        '''Return ``True`` if ``tag`` is in ``import_tags``.'''
        if exclude_tags:
            alltags = list(self._all_tags(tag))
            for exclude_tag in exclude_tags:
                for bit in alltags:
                    if bit == exclude_tag:
                        return 0
        if import_tags:
            c = 0
            alltags = list(self._all_tags(tag))
            for import_tag in import_tags:
                allitags = list(self._all_tags(import_tag))
                for bit in alltags:
                    if bit == import_tag:
                        return 2
                    elif bit in allitags:
                        c = 1
            return c
        else:
            return 2

    def _test_files(self, include, exclude):
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
            :return: a generator of tag, module path pairs.
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
            c = self._check_tag(tag, include_tags, exclude_tags)
            if not c:
                continue

            if is_file:
                yield tag, mod_path
            else:
                yield from self.get_tests(mod_path, include_tags,
                                          exclude_tags, m_tags)

    def import_module(self, file_name):
        return ModuleImporter(file_name, self.logger)

    def match(self, name):
        for pattern in test_patterns:
            p = pattern.search(name)
            if p:
                return p.groups(0)


class ModuleImporter:
    module = None
    add_to_path = False

    def __init__(self, file_name, logger):
        self.file_name = file_name
        self.logger = logger

    def __iter__(self):
        with self:
            if self.module:
                for name in dir(self.module):
                    obj = getattr(self.module, name)
                    if issubclass_safe(obj, unittest.TestCase):
                        yield obj

    def __enter__(self):
        path, name = os.path.split(self.file_name)
        add_to_path = path not in sys.path
        if add_to_path:
            sys.path.insert(0, path)
            self.add_to_path = add_to_path

        try:
            self.module = import_module(name[:-3])
        except ImportError:
            self.logger.error('Failed to import module %s. Skipping.',
                              name, exc_info=True)
            self.logger.debug('Full python path:\n%s', '\n'.join(sys.path))
        except Exception:
            self.logger.critical('Failed to import module %s. Skipping.',
                                 name, exc_info=True)
        return self

    def __exit__(self, *args):
        if self.add_to_path:
            sys.path.pop(0)
