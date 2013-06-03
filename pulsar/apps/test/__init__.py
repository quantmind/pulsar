'''The :class:`TestSuite` is a testing framework for
asynchronous/synchronous applications and for running tests in parallel
on multiple threads or processes.
It is used for testing pulsar itself but it can be used
as a test suite for any other library.

Requirements
====================
* unittest2_ for python 2.6
* mock_ for python < 3.3

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
                           ('examples','tests'))).start()

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

The next step is to actually write the tests.

Writing a Test Case
===========================
Only subclasses of  ``unittest.TestCase`` are collected by this application.
When running a test, pulsar looks for two extra method: ``_pre_setup`` and
``_post_teardown``. If the former is available, it is run just before the
``setUp`` method while if the latter is available, it is run
just after the ``tearDown`` method. In addition, if the ``setUpClass``
class methods is available, it is run just before
all tests functions are run and the ``tearDownClass``, if available, is run
just after all tests functions are run.

An example test case::

    # This import is equivalent in python2.6 to
    #     import unittest2 as unittest
    # Otherwise it is the same as
    #     import unittest
    from pulsar.apps.test import unittest
    
    class MyTest(unittest.TestCase):
        
        def test_async_test(self):
            result = yield maybe_async_function()
            self.assertEqual(result, ...)
            
        def test_simple_test(self):
            self.assertEqual(1, 1)

.. note::
    
    Test functions are asynchronous, when they return a generator or a
    :class:`pulsar.Deferred`, synchronous, when they return anything else.

.. _apps-test-loading:

Loading Tests
=================

Loading test cases is accomplished via the :class:`loader.TestLoader` class. In
this context we refer to an ``object`` as a ``module`` (including a
directory module) or a ``class``.

.. note::

    Test modules are specified when initialising a :class:`TestSuite`, by
    passing the ``modules`` tuple/list of python dotted paths. If not defined
    the default is to load test cases from the ``tests`` module.
    

These are the rules for loading tests:

* Directories that aren't packages are not inspected. This means that if a directory
  does not have a ``__init__.py`` file it won't be inspected.
* Any class that is a ``unittest.TestCase`` subclass is collected.
* If an object starts with ``_`` or ``.`` it won't be collected,
  nor will any objects it contains.
* If an object defines a ``__test__`` attribute that does not evaluate to
  ``True``, that object will not be collected, nor will any objects it contains.
  
The paths which will be inspected are defined by the **modules** initialisation
parameter for the :class:`TestSuite` class.
The **modules** parameter is a tuple or list of entries where each single entry
follow the following rules:

* If the entry is a string, it indicates the **dotted path** relative to the
  **root** directory (the directory of the script running the tests). For
  example::
  
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
* all tests from modules which starts with *test* from the ``examples`` directory.
     
All top level modules will be added to the python ``path``.


.. _test-suite-options:

Options
==================

All standard :ref:`settings <settings>` can be applied to the test application.
In addition, the following options are testsuite-specific:

.. _apps-test-sequential:

sequential
~~~~~~~~~~~~~~~~~~~
By default, test functions within a :class:`unittest.TestCase`
are run in asynchronous fashion. This means that several test functions
may be executed at once depending on their return values.
By specifying the ``--sequential`` command line option, the :class:`TestSuite`
forces tests to be run in a sequential model, one after the other::

    python runtests.py --sequential
    
Alternatively, if you need to specify a Testcase which always runs its test functions
in a sequential way, you can use the :func:`sequential` decorator::

    from pulsar.apps.test import unittest, sequential
    
    @sequential
    class MyTestCase(unittest.TestCase):
        ...
        
        
list labels
~~~~~~~~~~~~~~~~~~~
By passing the ``-l`` or ``--list-labels`` flag to the command line, the
full list of test labels available is displayed::

    python runtests.py -l
    

test timeout
~~~~~~~~~~~~~~~~~~~
When running asynchronous tests, it can be useful to set a cap on how
long a test function can wait for results. This is what the
``--test-timeout`` command line flag does::

    python runtests.py --test-timeout 10
    
Set the test timeout to 10 seconds.


Plugins
==================
A :class:`TestPlugin` is a way to extend the test suite with additional
:ref:`options <test-suite-options>` and behaviours implemented in
the various plugin's callbacks.
There are two basic rules for plugins:

* Plugin classes should subclass :class:`TestPlugin`.
* Plugins may implement any of the methods described in the class
  :class:`result.Plugin` interface.

Pulsar ships with two plugins:

.. _bench-plugin:

Benchmark
~~~~~~~~~~~~~

.. automodule:: pulsar.apps.test.plugins.bench


.. _profile-plugin:

Profile
~~~~~~~~~~~~~

.. automodule:: pulsar.apps.test.plugins.profile


.. module:: pulsar.apps.test

API
=========

.. autoclass:: TestSuite
   :members:
   :member-order: bysource

.. automodule:: pulsar.apps.test.loader

.. automodule:: pulsar.apps.test.result

.. module:: pulsar.apps.test


Test Plugin
~~~~~~~~~~~~~~~~~~~

.. autoclass:: TestPlugin
   :members:
   :member-order: bysource
   
.. _unittest2: http://pypi.python.org/pypi/unittest2
.. _mock: http://pypi.python.org/pypi/mock
'''
import logging
import time

import pulsar
from pulsar.apps import tasks
from pulsar.utils import events
from pulsar.utils.pep import ispy26, ispy33
from pulsar.utils.log import local_property

if ispy26: # pragma nocover
    try:
        import unittest2 as unittest
    except ImportError:
        print('To run tests in python 2.6 you need to install\
 the unittest2 package')
        exit(0)
else:
    import unittest
    
if ispy33: 
    from unittest import mock
else: # pragma nocover
    try:
        import mock
    except ImportError:
        print('To run tests you need to install the mock package')
        exit(0)

from .result import *
from .case import *
from .plugins.base import *
from .loader import *
from .utils import *
from .wsgi import *

def dont_run_with_thread(obj):
    '''Decorator for disabling test cases when the test suite runs in
threading, rather than processing, mode.'''
    actor = pulsar.get_actor()
    if actor:
        d = unittest.skipUnless(actor.cfg.concurrency=='process',
                                'Run only when concurrency is process')
        return d(obj)
    else:
        return obj


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
    default = 30
    desc = '''\
        Tests which take longer than this many seconds are timed-out
        and failed.'''


class TestLabels(TestOption):
    name = "labels"
    nargs = '*'
    validator = pulsar.validate_list
    desc = """Optional test labels to run. If not provided\
 all tests are run.

To see available labels use the -l option."""


class TestSize(TestOption):
    name = 'size'
    flags = ['--size']
    #choices = ('tiny','small','normal','big','huge')
    default = 'normal'
    desc = """Optional test size."""


class TestList(TestOption):
    name = "list_labels"
    flags = ['-l','--list-labels']
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
    desc = """Run test functions sequentially. Don't run them asynchronously."""
    

class TestSuite(tasks.TaskQueue):
    '''An asynchronous test suite which works like a task queue where each task
is a group of tests specified in a test class.

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
    Check the the :class:`TestLoader` for detailed information.

:parameter result_class: Optional class for collecting test results. By default
    it used the standard ``unittest.TextTestResult``.
:parameter plugins: Optional list of :class:`TestPlugin` instances.
'''
    name = 'test'
    cfg = pulsar.Config(apps=('tasks', 'test'),
                        loglevel='none',
                        task_paths=['pulsar.apps.test.case'],
                        plugins=(),
                        logconfig={
                                   'loggers': {
                                   'pulsar.test': {
                                        'handlers': ['console_message'],
                                        'level': logging.WARNING}
                                    }
                        })
    @local_property
    def runner(self):
        '''Instance of :class:`TestRunner` driving the test case
configuration and plugins.'''
        result_class = getattr(self, 'result_class', None)
        r = unittest.TextTestRunner()
        stream = r.stream
        runner = TestRunner(self.cfg.plugins, stream, result_class)
        abort_message = runner.configure(self.cfg)
        if abort_message:
            raise ExitTest(str(abort_message))
        return runner

    @local_property
    def loader(self):
        #When config is available load the tests and check what type of
        #action is required.
        modules = self.cfg.get('modules')
        # Create a runner and configure it
        runner = self.runner
        if not modules:
            modules = ['tests']
        if hasattr(modules, '__call__'):
            modules = modules(self)
        return TestLoader(self.root_dir, modules, runner, logger=self.logger)
    
    def on_config(self):
        loader = self.loader
        if self.cfg.list_labels:
            tags = self.cfg.labels
            if tags:
                s = '' if len(tags) == 1 else 's'
                print('\nTest labels for label{0} {1}\n'\
                      .format(s,', '.join(tags)))
            else:
                print('\nAll test labels\n')
            def _tags():
                for tag, mod in loader.testmodules(tags):
                    doc = mod.__doc__
                    if doc:
                        tag = '{0} - {1}'.format(tag,doc)
                    yield tag
            for tag in sorted(_tags()):
                print(tag)
            print('\n')
            return False
    
    def monitor_start(self, monitor):
        '''When the monitor starts load all test classes into the queue'''
        super(TestSuite, self).monitor_start(monitor)
        loader = self.local.loader
        tags = self.cfg.labels
        try:
            self.local.tests = tests = list(loader.testclasses(tags))
            self._time_start = None
            if tests:
                self.logger.info('loaded %s test classes', len(tests))
                self.runner.on_start()
                events.fire('tests', self, tests=tests)
                monitor.cfg.set('workers', min(self.cfg.workers, len(tests)))
                self._time_start = time.time()
                for tag, testcls in self.local.tests:
                    self.backend.run('test', testcls, tag)
                monitor.event_loop.call_every(self._check_queue)
            else:
                raise ExitTest('Could not find any tests.')
        except ExitTest as e:
            print(str(e))
            monitor.arbiter.stop()
        except Exception:
            self.logger.critical('Error occurred before starting tests',
                                 exc_info=True)
            monitor.arbiter.stop()

    @classmethod
    def create_config(cls, *args, **kwargs):
        cfg = super(TestSuite, cls).create_config(*args, **kwargs)
        if cfg.params.get('plugins') is None:
            cfg.params['plugins'] = ()
        for plugin in cfg.params['plugins']:
            cfg.settings.update(plugin.config.settings)
        return cfg
    
    @pulsar.async()
    def _check_queue(self):
        runner=  self.runner
        tests = yield self.backend.get_tasks(status=tasks.READY_STATES)
        if len(tests) == len(self.local.tests):
            time_taken = time.time() - self._time_start
            for task in tests:
                runner.add(task.result)
            runner.on_end()
            runner.printSummary(time_taken)
            # Shut down the arbiter
            if runner.result.errors or runner.result.failures:
                exit_code = 1
            else:
                exit_code = 0
            raise pulsar.HaltServer(exit_code=exit_code)