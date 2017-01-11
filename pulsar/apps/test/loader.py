'''
.. autoclass:: TestLoader
   :members:
   :member-order: bysource

'''
import os
import re
import sys
import unittest

from pulsar.api import get_stream
from pulsar.utils.importer import import_system_file

from .result import TestRunner


no_tags = set(('tests', 'test'))
build_directories = set(('build', 'dist', 'node_modules'))
test_patterns = [re.compile(r'''test_(?P<name>.*).py'''),
                 re.compile(r'''(?P<name>.*)_test.py'''),
                 re.compile(r'''tests.py''')]


def issubclass_safe(cls, base_cls):
    try:
        return issubclass(cls, base_cls)
    except TypeError:
        return False


def skip_module(mod_name):
    return (mod_name.startswith('_') or
            mod_name.startswith('.') or
            mod_name in build_directories)


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
        self.stream = get_stream(suite.cfg)
        self.runner = TestRunner(suite.cfg.test_plugins, self.stream)
        self.abort_message = self.runner.configure(suite.cfg)
        self.root = suite.root_dir
        self.modules = suite.cfg.test_modules
        self.logger = suite.logger

    def tags(self, include=None, exclude=None):
        """Return a generator of tag information
        """
        for tag, file_path_function in self.test_files(include, exclude):
            module = self.import_module(*file_path_function).module()
            if module:
                doc = module.__doc__
                if doc:
                    tag = '{0} - {1}'.format(tag, doc)
                yield tag

    def import_module(self, file_name, test_function=None):
        return ModuleImporter(file_name, test_function, self.logger)

    def test_files(self, include=None, exclude=None):
        """List of ``tag``, ``modules`` pairs.

        :parameter tags: optional list of tags to include, if not available
            all tags will be included.
        :parameter exclude_tags: optional list of tags to exclude.
            If not provided no tags will be excluded.
        """
        d = dict(self._test_files(include, exclude))
        return [(k, d[k]) for k in sorted(d)]

    # INTERNALS
    def _all_tags(self, tag):
        bits = tag.split('.')
        tag, rest = bits[0], bits[1:]
        yield tag
        for b in rest:
            tag += '.' + b
            yield tag

    def _check_tag(self, tag, include_tags, exclude_tags):
        '''Return ``True`` if ``tag`` is in ``import_tags``.'''
        if exclude_tags:
            alltags = list(self._all_tags(tag))
            for exclude_tag in exclude_tags:
                for bit in alltags:
                    if bit == exclude_tag:
                        return

        if include_tags:
            found = set()
            bits = tag.split('.')

            for tags in include_tags:
                tag = '.'.join(bits[:len(tags)])
                try:
                    index = tags.index(tag)
                except ValueError:
                    continue
                else:
                    remaining = tags[index+1:]
                    if len(remaining) == 1:
                        found.update(remaining)
                    elif not remaining:
                        return True
                    else:
                        return False

            return found
        else:
            return True

    def _test_files(self, include, exclude):
        if not self.modules:
            yield from self._get_tests(self.root, include, exclude)
        else:
            for name in self.modules:
                absolute_path = os.path.join(self.root, name)
                if os.path.isdir(absolute_path):
                    self.logger.debug('Loading from "%s"', name)
                    yield from self._get_tests(absolute_path, include, exclude)
                else:
                    raise ValueError('%s cannot be found in %s directory.'
                                     % (name, self.root))

    def _get_tests(self, path, include_tags, exclude_tags, tags=None):
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
            if include_tags:
                include_tags = [
                    list(self._all_tags(tag)) for tag in include_tags
                ]

        for mod_name in os.listdir(path):
            if skip_module(mod_name):
                continue
            mod_path = os.path.join(path, mod_name)
            is_file = os.path.isfile(mod_path)
            if is_file:
                tag = self._match(mod_name)
                if tag is None:  # does not match and is a file, skip.
                    continue
                m_tags = tags + list(tag)
            else:
                m_tags = tags if mod_name in no_tags else tags + [mod_name]

            tag = '.'.join(m_tags)

            if is_file:
                found = self._check_tag(tag, include_tags, exclude_tags)
                if isinstance(found, set):
                    for ttag in list(found):
                        test_function = ttag[len(tag)+1:]
                        if test_function.startswith('test'):
                            yield ttag, (mod_path, test_function)
                    continue
                elif found:
                    yield tag, (mod_path, None)
            else:
                if tag and include_tags:
                    bits = set()
                    for itags in include_tags:
                        bits.update(itags)
                    if tag not in bits:
                        continue
                yield from self._get_tests(mod_path, include_tags,
                                           exclude_tags, m_tags)

    def _match(self, name):
        for pattern in test_patterns:
            p = pattern.search(name)
            if p:
                return p.groups(0)


class ModuleImporter:

    def __init__(self, file_name, test_function, logger):
        self.file_name = file_name
        self.test_function = test_function
        self.logger = logger

    def __iter__(self):
        module = self.module()
        if module:
            test_function = self.test_function
            for name in dir(module):
                obj = getattr(module, name)
                if issubclass_safe(obj, unittest.TestCase):
                    if self.test_function and not hasattr(obj, test_function):
                        continue
                    yield obj, test_function

    def module(self):
        if not hasattr(self, '_module'):
            module = None
            try:
                module = import_system_file(self.file_name, False)
            except ImportError:
                self.logger.error('Failed to import module %s. Skipping.',
                                  self.file_name, exc_info=True)
                self.logger.debug('Full python path:\n%s', '\n'.join(sys.path))
            except Exception:
                self.logger.critical('Failed to import module %s. Skipping.',
                                     self.file_name, exc_info=True)
            self._module = module
        return self._module
