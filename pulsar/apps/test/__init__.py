'''\
An asynchronous parallel testing suite :class:`pulsar.Application`.
It is used for testing
pulsar itself and can be used as a test suite for any other library.

.. _apps-test-intro:

Introduction
====================

Create a script on the top level directory of your library,
let's call it ``runtests.py``::

    from pulsar.apps import TestSuite

    if __name__ == '__main__':
        TestSuite(description = 'Test suite for my library',
                  modules = ('regression',
                             ('examples','tests'))).start()

where the modules is an iterable for discovering test cases. Check the
:attr:`TestSuite.modules` attribute for more details.

In the above example
the test suite will look for all python files in the ``regression`` module
(in a recursive fashion), and for modules called ``tests`` in the `` example``
module.

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
'''
__test__ = False
import unittest
import logging
import os
import sys
import time
import inspect

if sys.version_info >= (2,7):
    import unittest
else:
    try:
        import unittest2 as unittest
    except ImportError:
        print('To run tests in python 2.6 you need to install\
 the unittest2 package')
        exit(0)

import pulsar
from pulsar.apps import tasks
from pulsar.async.commands import pulsar_command

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

@pulsar_command(internal=True, ack=False, commands_set=test_commands)
def test_result(client, actor, sender, tag, testcls, result):
    '''Command for sending test results from test workers to the test monitor.'''
    actor.log.debug('Got a test results from %s.%s', tag, testcls.__name__)
    actor.app.add_result(actor, result)


class TestSuite(tasks.CPUboundServer):
    '''An asynchronous test suite which works like a task queue where each task
is a group of tests specified in a test class.

:parameter modules: An iterable over modules where to look for tests. A module
    can be a string or a two-element tuple. For example::

        suite = TestSuite(modules = ('regression',
                                     ('examples','tests'),
                                     ('apps','test_*')))

    The :class:`TestLoader` will look into the ``regression`` module for all
    files and directories, while it will look into the example directory for all
    files or directories matching ``tests``.

    Alternatively it can be a callable returning the iterable over modules. The
    callable must accept one positional argument, the instance of the test
    suite::

        def get_modules(suite):
            ...

        suite = TestSuite(modules = get_modules)

:parameter result_class: Optional class for collecting test results. By default
    it used the standard ``unittest.TextTestResult``.
:parameter plugins: Optional list of :class:`Plugin` instances
'''
    _app_name = 'test'
    cfg_apps = ('cpubound',)
    commands_set = test_commands
    plugins = ()
    config_options_exclude = ('daemon','max_requests','user','group','pidfile')
    can_kill_arbiter = True
    cfg = {'loglevel': 'none', 'timeout': 3600, 'backlog': 1}

    def handler(self):
        return self

    def python_path(self):
        #Override the python path so that we put the directory where the script
        #is in the ppython path
        path = os.getcwd()
        if path not in sys.path:
            sys.path.insert(0, path)

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
        pulsar.arbiter()
        modules = getattr(self, 'modules', None)
        if not hasattr(self, 'plugins'):
            self.plugins = ()
        # Create a runner and configure it
        runner = self.runner
        if not modules:
            modules = ((None,'tests'),)
        if hasattr(modules,'__call__'):
            modules = modules(self)
        loader = TestLoader(os.getcwd(), modules, runner, logger=self.log)
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
        self.local.tests = tests = list(loader.testclasses(tags))
        if tests:
            self.log.info('loaded %s test classes', len(tests))
            self.runner.on_start()
            monitor.cfg.set('workers', min(self.cfg.workers, len(tests)))
            self._time_start = None
        else:
            print('Could not find any tests.')
            monitor.arbiter.stop()

    def monitor_task(self, monitor):
        if self._time_start is None:
            tests = self.local.tests
            self.log.info('sending %s test classes to the task queue',
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
            return monitor.arbiter.stop()

