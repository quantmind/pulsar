.. _apps-test:

.. module:: pulsar.apps.test

##############
Test Suite
##############

The :class:`TestSuite` is a testing framework for both synchronous and
asynchronous applications and for running tests in parallel
on multiple threads or processes.
It is used for testing pulsar but it can be used
as a test suite for any other library.

.. contents::
   :depth: 3

.. _apps-test-intro:

===============
Integration
===============

Pulsar test suite can be used in conjunction with setuptools_ or stand-alone.

Setuptools Integration
===========================

Pulsar asynchronous test suite can be used as testing framework in
your setuptools based project.
Add this to setup.py file::

    from setuptools import setup

    setup(
        #...,
        setup_requires=['pulsar', ...],
        #...,
    )

And create an alias into ``setup.cfg`` file::

    [aliases]
    test=pulsar_test

If you now type::

    python setup.py test

this will execute your tests using pulsar test runner. As this is a
standalone version of pulsar no prior installation whatsoever is
required for calling the test command.
You can also pass additional arguments to pulsar test,
such as your test directory or other options using -a.


Manual Integration
===========================

If for some reason you don’t want/can’t use the setuptools integration,
you can write a standalone script, for example ``runtests.py``::

    from pulsar.apps import TestSuite

    if __name__ == '__main__':
        TestSuite(description='Test suite for my library').start()

To run the test suite::

    python runtests.py

Type::

    python runtests.py --help

For a list different options/parameters which can be used when running tests.

===========================
Running Tests
===========================


Writing a Test Case
===========================

Only subclasses of  :class:`~unittest.TestCase` are collected by this
application. An example test case::

    import unittest

    class MyTest(unittest.TestCase):

        async def test_async_test(self):
            result = await async_function()
            self.assertEqual(result, ...)

        def test_simple_test(self):
            self.assertEqual(1, 1)

.. note::

    Test functions are asynchronous, when they are coroutine functions or
    return a :class:`~asyncio.Future`, synchronous, when they
    return anything else.

.. _apps-test-loading:

Loading Tests
=================

The loading of test cases is controlled by the ``modules`` parameter when
initialising the :class:`TestSuite`::

    from pulsar.apps import TestSuite

    if __name__ == '__main__':
        TestSuite(modules=('tests', 'examples')).start()

When using pulsar test suite - setuptools_ integration, test modules are specified
in the ``setup.cfg``::

    [test]
    test_modules = tests examples

Test files
===========================

The :class:`.TestSuite` loads tests via the :attr:`~.TestSuite.loader`
property. By default it recursively loads test files inside the
``modules`` directories matching:

* ``test_<label>.py``
* ``<label>_test.py``
* ``tests.py``

Test labels
======================

To run a specific label::

    python runtests.py <label>

When using the setuptools_ integration::

    python setup.py test -a <label>

To list all labels::

    python setup.py test -l

It is also possible to run a single test function within a label::

   python setup.py test -a <label>.<test_function_name>


.. _test-suite-options:

==================
Options
==================

All standard :ref:`settings <settings>` can be applied to the test application.
In addition, the following options are
:ref:`test suite specific <setting-section-test>`:

.. _apps-test-sequential:

sequential
==================

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


list labels
==================

By passing the ``-l`` or :ref:`--list-labels <setting-list_labels>` flag
to the command line, the full list of test labels available is displayed::

    python runtests.py -l


test timeout
==================

When running asynchronous tests, it can be useful to set a cap on how
long a test function can wait for results. This is what the
:ref:`--test-timeout <setting-test_timeout>` command line flag does::

    python runtests.py --test-timeout 10

Set the test timeout to 10 seconds.
Test timeout is only meaningful for asynchronous test function.

==================
Http TestClient
==================

The :class:`.HttpTestClient` can be used to test ``wsgi`` middleware without going
through socket connections.

To use the client in a test function::

   async def test_my_wsgi_test(self):
      http = HttpTestClient(self, wsgi)
      response = await http.get('http://bla.com/...')

The host part of the url is irrelevant, it can be anything you like.

==================
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
==================

.. automodule:: pulsar.apps.test.plugins.bench


.. _profile-plugin:

Profile
==================

.. automodule:: pulsar.apps.test.plugins.profile

=========
API
=========

Test Suite
==================

.. autoclass:: pulsar.apps.test.TestSuite
   :members:
   :member-order: bysource


Test Loader
==================

.. automodule:: pulsar.apps.test.loader


Plugin
==================

.. autoclass:: pulsar.apps.test.result.Plugin
   :members:
   :member-order: bysource


Test Runner
==================

.. autoclass:: pulsar.apps.test.result.TestRunner
   :members:
   :member-order: bysource


Test Result
==================

.. autoclass:: pulsar.apps.test.result.TestResult
   :members:
   :member-order: bysource


Test Plugin
==================

.. autoclass:: pulsar.apps.test.plugins.base.TestPlugin
   :members:
   :member-order: bysource


Http Test Client
==================

.. autoclass:: pulsar.apps.test.wsgi.HttpTestClient
   :members:
   :member-order: bysource

Populate
==================

A useful function for populating random data::

    from pulsar.apps.test import populate

    data = populate('string', 100)

gives you a list of 100 random strings


.. autofunction:: pulsar.apps.test.populate.populate

================
Utilities
================

.. automodule:: pulsar.apps.test.utils


.. _setuptools: https://pythonhosted.org/setuptools/index.html
