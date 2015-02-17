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


def issubclass_safe(cls, base_cls):
    try:
        return issubclass(cls, base_cls)
    except TypeError:
        return False


class TestLoader(object):
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
        '''Generator of ``tag``, ``modules`` pairs.

:parameter tags: optional list of tags to include, if not available all tags
    will be included.
:parameter exclude_tags: optional list of tags to exclude. If not provided no
    tags will be excluded.'''
        d = dict(self._testmodules(tags, exclude_tags))
        return [(k, d[k]) for k in sorted(d)]

    def _testmodules(self, tags, exclude_tags):
        for name, pattern, tag in self.modules:
            names = name.split('.') if name else ()
            absolute_path = pattern_path = os.path.join(self.root, *names)
            if pattern == '*':
                pattern = None
            if pattern:
                pattern_path = os.path.join(pattern_path, pattern)
                pattern = re.compile(pattern.replace('*', '(.*)'))
            self.logger.debug('Loading from "%s"', pattern_path)
            if os.path.isdir(absolute_path):
                pathbase = os.path.dirname(absolute_path)
                if pathbase not in sys.path:
                    sys.path.append(pathbase)
                name = names[-1]
                stags = (tag,) if tag else ()
                for tag, mod in self.get_tests(absolute_path, name, pattern,
                                               import_tags=tags, tags=stags,
                                               exclude_tags=exclude_tags):
                    yield tag, mod
            elif os.path.isfile(absolute_path + '.py'):
                include, ntag = self.match(pattern,
                                           os.path.basename(absolute_path))
                if include:
                    tag = ntag or tag or name
                    mod = self.import_module(name)
                    if mod:
                        yield tag[0] if isinstance(tag, tuple) else tag, mod
            else:
                raise ValueError('%s cannot be found in %s directory.'
                                 % (name, self.root))

    def get_tests(self, path, dotted_path, pattern, import_tags=None,
                  tags=(), exclude_tags=None, parent=None):
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
            is_file = os.path.isfile(mod_path)
            if is_file:
                if mod_name.endswith('.py'):
                    mod_name = mod_name.split('.')[0]
                else:
                    continue
            include, addtag = self.match(pattern, mod_name)
            if not include and is_file:  # does not match and is a file, skip.
                continue
            elif include and not is_file and pattern:
                # All modules under this directory will be included
                # regardless of pattern
                pattern = None
            # module dotted path
            if dotted_path:
                mod_dotted_path = '%s.%s' % (dotted_path, mod_name)
            else:
                tags = (mod_name,)
                mod_dotted_path = mod_name
            #
            module = self.import_module(mod_dotted_path, mod_path, parent)
            if not module:
                continue
            ctags = tags + addtag
            tag = '.'.join(ctags)
            c = self.checktag(tag, import_tags, exclude_tags)
            if not c:
                continue
            if is_file:
                yield tag, module
            else:
                counter = 0
                # Recursively import modules
                for ctag, mod in self.get_tests(mod_path, mod_dotted_path,
                                                pattern, import_tags, ctags,
                                                exclude_tags, parent=module):
                    counter += 1
                    yield ctag, mod
                # If more than one submodule, yield this tag too
                if pattern:
                    if counter > 1:
                        yield tag, module
                elif c == 2:
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
                self.logger.error('Failed to import module %s. Skipping.',
                                  name, exc_info=True)
                self.logger.debug('Full python path:\n%s', '\n'.join(sys.path))
            except Exception:
                self.logger.critical('Failed to import module %s. Skipping.',
                                     name, exc_info=True)

    def match(self, pattern, name):
        if pattern:
            p = pattern.search(name)
            if p:
                return True, p.groups(0)
            else:
                return False, (name,)
        else:
            return True, (name,)
