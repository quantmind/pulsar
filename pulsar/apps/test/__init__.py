'''\
An asynchronous parallel testing suite :class:`pulsar.Application`.
Used for testing pulsar itself but it can be used as a test suite for
any other library.

Requirements
====================
* unittest2_ for python 2.6
* mock_ for python < 3.3

.. _apps-test-intro:

Introduction
====================

Create a script on the top level directory of your library,
let's call it ``runtests.py``::

    from pulsar.apps import TestSuite

    if __name__ == '__main__':
        TestSuite(description='Test suite for my library',
                  modules=('regression',
                           ('examples','tests'))).start()

where ``modules`` is an iterable for discovering test cases. Check the
:class:`TestLoader` for details.
In the above example the test suite will look for all python files
in the ``regression`` module (in a recursive fashion), and for modules
called ``tests`` in the ``example`` module.

.. _apps-test-loading:

Loading Tests
=================

Loading test cases is accomplished via the :class:`TestLoader` class. In
this context we refer to an ``object`` as a ``module`` (including a
directory module) or a ``class``.

These are the rules for loading tests:

* Directories that aren't packages are not inspected.
* Any class that is a ``unittest.TestCase`` subclass is collected.
* if an object starts with ``_`` or ``.`` it won't be collected,
  nor will any objects it contains.
* If an object defines a ``__test__`` attribute that does not evaluate to True,
  that object will not be collected, nor will any objects it contains.


Test Case
=============
Only subclasses of  ``unittest.TestCase`` are collected by this application.
When running a test, pulsar looks for two extra method: ``_pre_setup`` and
``_post_teardown``. If the former is available, it is run just before the
``setUp`` method while if the latter is available, it is run
just after the ``tearDown`` method.

.. _unittest2: http://pypi.python.org/pypi/unittest2
.. _mock: http://pypi.python.org/pypi/mock
'''
__test__ = False
import logging
import os
import sys
import time
import inspect

if sys.version_info >= (2,7):
    import unittest
else:   # pragma nocover
    try:
        import unittest2 as unittest
    except ImportError:
        print('To run tests in python 2.6 you need to install\
 the unittest2 package')
        exit(0)

if sys.version_info < (3,3): # pragma nocover
    try:
        import mock
    except ImportError:
        print('To run tests you need to install the mock package')
        exit(0)
else:
    from unittest import mock
    
import pulsar
from pulsar.apps import tasks
from pulsar.utils import events

from .result import *
from .case import *
from .plugins.base import *
from .loader import *
from .utils import *
from .wsgi import *

def dont_run_with_thread(obj):
    c = pulsar.get_actor().cfg.concurrency
    d = unittest.skipUnless(c=='process',
                            'Run only when concurrency is process')
    return d(obj)


class ExitTest(Exception):
    pass


class TestVerbosity(TestOption):
    name = 'verbosity'
    flags = ['--verbosity']
    type = int
    default = 1
    desc = """Test verbosity, 0, 1, 2, 3"""


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
    flags = ['-l','--list_labels']
    action = 'store_true'
    default = False
    validator = pulsar.validate_bool
    desc = """List all test labels without performing tests."""


test_commands = set()

@pulsar.command(internal=True, ack=False, commands_set=test_commands)
def test_result(client, actor, sender, tag, clsname, result):
    '''Command for sending test results from test workers to the test monitor.'''
    actor.logger.debug('Got a test results from %s.%s', tag, clsname)
    return actor.app.add_result(actor, result)


class TestSuite(tasks.CPUboundServer):
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
    _app_name = 'test'
    cfg_apps = ('cpubound',)
    commands_set = test_commands
    cfg = {'loglevel': 'none',
           'timeout': 3600,
           'backlog': 1,
           'logconfig': {
                'loggers': {
                    LOGGER.name: {'handlers': ['console_message'],
                                  'level': logging.INFO}
                            }
                         }
           }

    def handler(self):
        return self

    def python_path(self):
        #Override the python path so that we put the directory where the script
        #is in the ppython path
        path = os.getcwd()
        if path not in sys.path:
            sys.path.insert(0, path)
    
    def on_config_init(self, cfg, params):
        self.plugins = params.get('plugins') or ()
        if self.plugins:
            for plugin in self.plugins:
                cfg.settings.update(plugin.config.settings)
    
    @property
    def runner(self):
        '''Instance of :class:`TestRunner` driving the test case
configuration and plugins.'''
        if 'runner' not in self.local:
            result_class = getattr(self, 'result_class', None)
            r = unittest.TextTestRunner()
            stream = r.stream
            runner = TestRunner(self.plugins, stream, result_class)
            abort_message = runner.configure(self.cfg)
            if abort_message:
                raise ExitTest(str(abort_message))
            self.local.runner = runner
        return self.local.runner

    def on_config(self):
        #When config is available load the tests and check what type of
        #action is required.
        modules = getattr(self, 'modules', None)
        if not hasattr(self, 'plugins'):
            self.plugins = ()
        # Create a runner and configure it
        runner = self.runner
        if not modules:
            modules = ['tests']
        if hasattr(modules, '__call__'):
            modules = modules(self)
        loader = TestLoader(os.getcwd(), modules, runner, logger=self.logger)
        # Listing labels
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
        self.local.loader = loader

    def monitor_start(self, monitor):
        # When the monitor starts load all :class:`TestRequest` into the
        # in the :attr:`pulsar.Actor.ioqueue`.
        loader = self.local.loader
        tags = self.cfg.labels
        try:
            self.local.tests = tests = list(loader.testclasses(tags))
            if tests:
                self.logger.info('loaded %s test classes', len(tests))
                self.runner.on_start()
                events.fire('tests', self, tests=tests)
                monitor.cfg.set('workers', min(self.cfg.workers, len(tests)))
                self._time_start = None
            else:
                raise ExitTest('Could not find any tests.')
        except ExitTest as e:
            print(str(e))
            monitor.arbiter.stop()
        except Exception:
            LOGGER.critical('Error occurred before starting tests',
                            exc_info=True)
            monitor.arbiter.stop()

    def monitor_task(self, monitor):
        if self._time_start is None:
            tests = self.local.tests
            self.logger.info('sending %s test classes to the task queue',
                          len(tests))
            self._time_start = time.time()
            for tag, testcls in tests:
                monitor.put(TestRequest(testcls, tag))

    def add_result(self, monitor, result):
        #Check if we got all results
        runner = self.runner
        runner.add(result)
        if runner.count == len(self.local.tests):
            time_taken = time.time() - self._time_start
            runner.on_end()
            runner.printSummary(time_taken)
            # Shut down the arbiter
            if runner.result.errors or runner.result.failures:
                exit_code = 1
            else:
                exit_code = 0 
            return monitor.arbiter.stop(exit_code=exit_code)

