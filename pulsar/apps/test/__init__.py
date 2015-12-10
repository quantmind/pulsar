'''The :class:`TestSuite` is a testing framework for both synchronous and
asynchronous applications and for running tests in parallel
on multiple threads or processes.
It is used for testing pulsar but it can be used
as a test suite for any other library.

.. _apps-test-intro:

Introduction
====================

To get started with pulsar asynchronous test suite is easy. The first thing to
do is to create a python ``script`` on the top level directory of your library,
let's call it ``runtests.py``::

    from pulsar.apps import TestSuite

    if __name__ == '__main__':
        TestSuite(description='Test suite for my library',
                  modules=('regression',
                           ('examples', 'tests'))).start()

where ``modules`` is a tuple/list which specifies where to
:ref:`search for test cases <apps-test-loading>` to run.
In the above example the test suite will look for all python files
in the ``regression`` module (in a recursive fashion), and for modules
called ``tests`` in the ``examples`` module.

To run the test suite::

    python runtests.py

Type::

    python runtests.py --help

For a list different options/parameters which can be used when running tests.

.. note::

    Pulsar test suite help you speed up your tests!
    Use ``setUpClass`` rather than ``setUp``, use asynchronous tests and if
    not possible add fire power with more test workers.

    Try with the ``-w 8`` command line input for a laugh.

The next step is to actually write the tests.

Writing a Test Case
===========================
Only subclasses of  :class:`~unittest.TestCase` are collected by this
application.
When running a test, pulsar looks for two extra method: ``_pre_setup`` and
``_post_teardown``. If the former is available, it is run just before the
:meth:`~unittest.TestCase.setUp` method while if the latter is available,
it is run just after the :meth:`~unittest.TestCase.tearDown` method.
In addition, if the :meth:`~unittest.TestCase.setUpClass`
class methods is available, it is run just before
all tests functions are run and the :meth:`~unittest.TestCase.tearDownClass`,
if available, is run just after all tests functions are run.

An example test case::

    import unittest

    class MyTest(unittest.TestCase):

        def test_async_test(self):
            result = yield maybe_async_function()
            self.assertEqual(result, ...)

        def test_simple_test(self):
            self.assertEqual(1, 1)

.. note::

    Test functions are asynchronous, when they return a generator or a
    :class:`~asyncio.Future`, synchronous, when they return anything else.

.. _apps-test-loading:

Loading Tests
=================

The loading of test cases is controlled by the ``modules`` parameter when
initialising the :class:`TestSuite`::

    from pulsar.apps import TestSuite

    if __name__ == '__main__':
        TestSuite(modules=('tests',
                          ('examples','tests'))).start()

The :class:`TestSuite` loads tests via the :class:`~loader.TestLoader` class.

In the context of explaining the rules for loading tests, we refer to
an ``object`` as

* a ``module`` (including a directory module)
* a python ``class``.

with this in mind:

* Directories that aren't packages are not inspected. This means that if a
  directory does not have a ``__init__.py`` file, it won't be inspected.
* Any class that is a :class:`~unittest.TestCase` subclass is collected.
* If an ``object`` starts with ``_`` or ``.`` it won't be collected,
  nor will any ``objects`` it contains.
* If an ``object`` defines a ``__test__`` attribute that does not evaluate to
  ``True``, that object will not be collected, nor will any ``objects``
  it contains.

The ``modules`` parameter is a tuple or list of entries where each single
``entry`` follows the rules:

* If the ``entry`` is a string, it indicates the **dotted path** relative
  to the **root** directory (the directory of the script running the tests).
  For example::

      modules = ['tests', 'moretests.here']

* If an entry is two elements tuple, than the first element represents
  a **dotted path** relative to the **root** directory and the
  second element a **pattern** which modules (files or directories) must match
  in order to be included in the search. For example::

      modules = [...,
                 ('examples', 'test*')
                 ]

  load modules, from inside the ``examples`` module, starting with ``test``.

* If an entry is a three elements tuple, it is the same as the *two elements
  tuple* rule with the third element which specifies the top level **tag**
  for all tests in this entry. For example::

      modules = [...,
                  ('bla', '*', 'foo')
                ]

For example, the following::

    modules = ['test', ('bla', '*', 'foo'), ('examples','test*')]

loads

* all tests from modules in the ``test`` directory,
* all tests from the ``bla`` directory with top level tag ``foo``,
* all tests from modules which starts with *test* from the ``examples``
  directory.

All top level modules will be added to the python ``path``.


.. _test-suite-options:

Options
==================

All standard :ref:`settings <settings>` can be applied to the test application.
In addition, the following options are
:ref:`test suite specific <setting-section-test>`:

.. _apps-test-sequential:

sequential
~~~~~~~~~~~~~~~~~~~
By default, test functions within a :class:`~unittest.TestCase`
are run in asynchronous fashion. This means that several test functions
may be executed at once depending on their return values.

By specifying the :ref:`--sequential <setting-sequential>` command line option,
the :class:`.TestSuite` forces test functions from a given
:class:`~unittest.TestCase` to be run in a sequential way,
one after the other::

    python runtests.py --sequential

Alternatively, if you need to specify a :class:`~unittest.TestCase` which
always runs its test functions in a sequential way, you can use
the :func:`.sequential` decorator::

    from pulsar.apps.test import sequential

    @sequential
    class MyTestCase(unittest.TestCase):
        ...

Using the ``sequential`` option, does not mean only one test function
is executed by the :class:`.TestSuite` at a given time. Indeed, several
:class:`~unittest.TestCase` are executed at the same time and therefore
each one of the may have one test function running.

In order to run only one test function at any time, the ``sequential``
option should be used::

    python runtests.py --sequential

list labels
~~~~~~~~~~~~~~~~~~~
By passing the ``-l`` or :ref:`--list-labels <setting-list_labels>` flag
to the command line, the full list of test labels available is displayed::

    python runtests.py -l


test timeout
~~~~~~~~~~~~~~~~~~~
When running asynchronous tests, it can be useful to set a cap on how
long a test function can wait for results. This is what the
:ref:`--test-timeout <setting-test_timeout>` command line flag does::

    python runtests.py --test-timeout 10

Set the test timeout to 10 seconds.


Test Plugins
==================
A :class:`.TestPlugin` is a way to extend the test suite with additional
:ref:`options <test-suite-options>` and behaviours implemented in
the various plugin's callbacks.
There are two basic rules for plugins:

* Test plugin classes should subclass :class:`.TestPlugin`.
* Test plugins may implement any of the methods described in the class
  :class:`.Plugin` interface.

Pulsar ships with two battery-included plugins:

.. _bench-plugin:

Benchmark
~~~~~~~~~~~~~

.. automodule:: pulsar.apps.test.plugins.bench


.. _profile-plugin:

Profile
~~~~~~~~~~~~~

.. automodule:: pulsar.apps.test.plugins.profile


API
=========

Test Suite
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.test.TestSuite
   :members:
   :member-order: bysource


Test Loader
~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: pulsar.apps.test.loader


Plugin
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.test.result.Plugin
   :members:
   :member-order: bysource


Test Runner
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.test.result.TestRunner
   :members:
   :member-order: bysource


Test Result
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.test.result.TestResult
   :members:
   :member-order: bysource


Test Plugin
~~~~~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.test.plugins.base.TestPlugin
   :members:
   :member-order: bysource

Populate
~~~~~~~~~~~~~

A useful function for populating random data::

    from pulsar.apps.test import populate

    data = populate('string', 100)

gives you a list of 100 random strings


.. autofunction:: pulsar.apps.test.populate.populate


Utilities
================

.. automodule:: pulsar.apps.test.utils

'''
import sys

import pulsar
from pulsar.utils.log import lazyproperty
from pulsar.utils.config import section_docs, TestOption

from .populate import populate, random_string
from .result import *           # noqa
from .plugins.base import *     # noqa
from .loader import *           # noqa
from .utils import *            # noqa
from .wsgi import *             # noqa
from .runner import Runner


__all__ = ['populate', 'random_string']


pyver = '%s.%s' % (sys.version_info[:2])

section_docs['Test'] = '''
This section covers configuration parameters used by the
:ref:`Asynchronous/Parallel test suite <apps-test>`.'''


class ExitTest(Exception):
    pass


class TestVerbosity(TestOption):
    name = 'verbosity'
    flags = ['--verbosity']
    type = int
    default = 1
    desc = """Test verbosity, 0, 1, 2, 3"""


class TestTimeout(TestOption):
    name = 'test_timeout'
    flags = ['--test-timeout']
    validator = pulsar.validate_pos_int
    type = int
    default = 5
    desc = '''\
        Tests which take longer than this many seconds are timed-out
        and failed.'''


class TestLabels(TestOption):
    name = "labels"
    nargs = '*'
    validator = pulsar.validate_list
    desc = """\
        Optional test labels to run.

        If not provided all tests are run.
        To see available labels use the ``-l`` option."""


class TestExcludeLabels(TestOption):
    name = "exclude_labels"
    flags = ['-e', '--exclude-labels']
    nargs = '*'
    desc = 'Exclude a group o labels from running.'
    validator = pulsar.validate_list


class TestSize(TestOption):
    name = 'size'
    flags = ['--size']
    choices = ('tiny', 'small', 'normal', 'big', 'huge')
    default = 'normal'
    desc = """Optional test size."""


class TestList(TestOption):
    name = "list_labels"
    flags = ['-l', '--list-labels']
    action = 'store_true'
    default = False
    validator = pulsar.validate_bool
    desc = """List all test labels without performing tests."""


class TestSequential(TestOption):
    name = "sequential"
    flags = ['--sequential']
    action = 'store_true'
    default = False
    validator = pulsar.validate_bool
    desc = """Run test functions sequentially."""


class TestShowLeaks(TestOption):
    name = "show_leaks"
    flags = ['--show-leaks']
    nargs = '?'
    choices = (0, 1, 2)
    const = 1
    type = int
    default = 0
    desc = """Shows memory leaks.

    Run the garbage collector before a process-based actor dies and shows
    the memory leak report.
    """


class TestSuite(pulsar.Application):
    '''An asynchronous test suite which works like a task queue.

    Each task is a group of test methods in a python TestCase class.

    :parameter modules: An iterable over modules where to look for tests.
        Alternatively it can be a callable returning the iterable over modules.
        For example::

            suite = TestSuite(modules=('regression',
                                       ('examples','tests'),
                                       ('apps','test_*')))

            def get_modules(suite):
                ...

            suite = TestSuite(modules=get_modules)

        If not provided it is set as default to ``["tests"]`` which loads all
        python module from the tests module in a recursive fashion.
        Check the the :class:`.TestLoader` for detailed information.

    :parameter result_class: Optional class for collecting test results.
        By default it used the standard :class:`.TestResult`.
    :parameter plugins: Optional list of :class:`.TestPlugin` instances.
    '''
    name = 'test'
    cfg = pulsar.Config(apps=['test'],
                        loglevel=['none'],
                        plugins=())

    def new_runner(self):
        '''The :class:`.TestRunner` driving test cases.
        '''
        result_class = getattr(self, 'result_class', None)
        stream = pulsar.get_stream(self.cfg)
        runner = TestRunner(self.cfg.plugins, stream, result_class)
        abort_message = runner.configure(self.cfg)
        if abort_message:    # pragma    nocover
            raise ExitTest(str(abort_message))
        self.runner = runner
        return runner

    @lazyproperty
    def loader(self):
        # When config is available load the tests and check what type of
        # action is required.
        modules = self.cfg.get('modules')
        # Create a runner and configure it
        runner = self.new_runner()
        if not modules:
            modules = ['tests']
        if hasattr(modules, '__call__'):
            modules = modules(self)
        return TestLoader(self.root_dir, modules, runner, logger=self.logger)

    def on_config(self, arbiter):
        stream = arbiter.stream
        try:
            loader = self.loader
        except ExitTest as e:
            stream.writeln(str(e))
            return False
        stream = arbiter.stream
        stream.writeln(sys.version)
        if self.cfg.list_labels:    # pragma    nocover
            tags = self.cfg.labels
            if tags:
                s = '' if len(tags) == 1 else 's'
                stream.writeln('\nTest labels for%s %s:' %
                               (s, ', '.join(tags)))
            else:
                stream.writeln('\nAll test labels:')
            stream.writeln('')

            def _tags():
                for tag, mod in loader.testmodules(tags):
                    doc = mod.__doc__
                    if doc:
                        tag = '{0} - {1}'.format(tag, doc)
                    yield tag
            for tag in sorted(_tags()):
                stream.writeln(tag)
            stream.writeln('')
            return False

    def monitor_start(self, monitor):
        '''When the monitor starts load all test classes into the queue'''
        cfg = self.cfg
        workers = min(0, cfg.workers)
        cfg.set('workers', workers)
        loader = self.loader

        if self.cfg.callable:
            self.cfg.callable()

        tags = self.cfg.labels
        exclude_tags = self.cfg.exclude_labels
        if self.cfg.show_leaks:
            show = show_leaks if self.cfg.show_leaks == 1 else hide_leaks
            self.cfg.set('when_exit', show)
            arbiter = pulsar.arbiter()
            arbiter.cfg.set('when_exit', show)
        try:
            tests = []
            loader.runner.on_start()
            for tag, testcls in loader.testclasses(tags, exclude_tags):
                suite = loader.runner.loadTestsFromTestCase(testcls)
                if suite and suite._tests:
                    tests.append((tag, testcls))
            self._time_start = None
            if tests:
                self.logger.info('loading %s test classes', len(tests))
                monitor._loop.call_soon(Runner, monitor, loader.runner, tests)
            else:   # pragma    nocover
                raise ExitTest('Could not find any tests.')
        except ExitTest as e:   # pragma    nocover
            monitor.stream.writeln(str(e))
            monitor._loop.stop()
        except Exception:   # pragma    nocover
            monitor.logger.critical('Error occurred while starting tests',
                                    exc_info=True)
            monitor._loop.call_soon(self._exit, 3)

    @classmethod
    def create_config(cls, *args, **kwargs):
        cfg = super(TestSuite, cls).create_config(*args, **kwargs)
        if cfg.params.get('plugins') is None:
            cfg.params['plugins'] = ()
        for plugin in cfg.params['plugins']:
            cfg.settings.update(plugin.config.settings)
        return cfg

    def arbiter_params(self):
        params = super().arbiter_params()
        params['concurrency'] = self.cfg.concurrency
        return params
