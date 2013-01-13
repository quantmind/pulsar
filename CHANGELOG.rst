Ver. 0.4.4 - 2013-Jan-13
==============================
* Documentation for development version hosted on github.
* Modified :meth:`pulsar.Actor.exit` so that it shuts down :attr:`pulsar.Actor.mailbox`
  after closing the :attr:`pulsar.Actor.requestloop`.
* Fixed bug which prevented :ref:`daemonisation <setting-daemon>` in posix systems.
* Changed the :meth:`pulsar.Deferred.result_or_self` method to return the
  *result* when the it is called and no callbacks are available.
  It avoids several unnecessary calls on deeply nested :class:`pulsar.Deferred`
  (which sometimes caused maximum recursion depth exceeded).
* Fixed calculator example script.
* **374 regression tests**, **87% coverage**.

Ver. 0.4.3 - 2012-Dec-28
==============================
* Removed the tasks in event loop. A task can only be added by appending
  callbacks or timeouts.
* Fixed critical bug in :class:`pulsar.MultiDeferred`.
* Test suite works with multiple test workers.
* Fixed issue #17 on asynchronous shell application.
* Dining philosophers example works on events only.
* Removed obsolete safe_monitor decorator in :mod:`pulsar.apps`.
* **365 regression tests**, **87% coverage**.

Ver. 0.4.2 - 2012-Dec-12
==============================
* Fixed bug in boolean validation.
* Refactored :class:`pulsar.apps.test.TestPlugin` to handle multi-parameters.
* Removed unused code and increased test coverage.
* **338 regression tests**, **86% coverage**.

Ver. 0.4.1 - 2012-Dec-04
==============================
* Test suite can load test from single files as well as directories.
* :func:`pulsar.apps.wsgi.handle_wsgi_error` accepts optional ``content_type``
  and ``encoding`` parameters.
* Fix issue #20, test plugins not included are not available in the command line.
* :class:`pulsar.Application` call :meth:`pulsar.Config.on_start` before starting.
* **304 regression tests**, **83% coverage**.

Ver. 0.4 - 2012-Nov-19
============================
* Overall refactoring of API and therefore incompatible with previous versions.
* Development status set to ``Beta``.
* Support pypy_ and python 3.3.
* Added the new :mod:`pulsar.utils.httpurl` module for HTTP tools and HTTP 
  synchronous and asynchronous clients.
* Refactored :class:`pulsar.Deferred` to be more compatible with twisted. You
  can add separate callbacks for handling errors.
* Added :class:`pulsar.MultiDeferred` for handling a group of asynchronous
  elements independent from each other.
* The :class:`pulsar.Mailbox` does not derive from :class:`threading.Thread` so
  that the eventloop can be restarted.
* Removed the :class:`ActorMetaClass`. Remote functions are specified using
  a dictionary.
* Socket and WSGI :class:`pulsar.Application` are built on top of the new
  :class:`pulsar.AsyncSocketServer` framework class.
* **303 regression tests**, **83% coverage**.

Ver. 0.3 - 2012-May-03
============================
* Development status set to ``Alpha``.
* This version brings several bug fixes, more tests, more docs, and improvements
  in the :mod:`pulsar.apps.tasks` application.
* Added :meth:`pulsar.apps.tasks.Job.send_to_queue` method for allowing
  :meth:`pulsar.apps.tasks.Task` to create new tasks. 
* The current :class:`pulsar.Actor` is always available on the current thread
  ``actor`` attribute.
* Trap errors in :meth:`pulsar.IOLoop.do_loop_tasks` to avoid having monitors
  crashing the arbiter.
* Added :func:`pulsar.system.system_info` function which returns system information
  regarding a running process. It requires psutil_.
* Added global :func:`pulsar.spawn` and :func:`pulsar.send` functions for
  creating and communicating between :class:`pulsar.Actor`.
* Fixed critical bug in :meth:`pulsar.net.HttpResponse.default_headers`.
* Added :meth:`pulsar.utils.http.Headers.pop` method.
* Allow :attr:`pulsar.apps.tasks.Job.can_overlap` to be a callable.
* Added :attr:`pulsar.apps.tasks.Job.doc_syntax` attribute which defaults to
  ``"markdown"``.
* :class:`pulsar.Application` can specify a version which overrides
  :attr:`pulsar.__version__`.
* Added Profile test plugin to :ref:`test application <apps-test>`.
* Task scheduler check for expired tasks via the
  :meth:`pulsar.apps.tasks.Task.check_unready_tasks` method.
* PEP 386-compliant version number.
* Setup does not fail when C extensions fail to compile.
* **95 regression tests**, **75% coverage**.

Ver. 0.2.1 - 2011-Dec-18
=======================================
* Catch errors in :func:`pulsar.apps.test.run_on_arbiter`.
* Added new setting for configuring http responses when an unhandled error
  occurs (Issue #7). 
* It is possible to access the actor :attr:`pulsar.Actor.ioloop` form the
  current thread ``ioloop`` attribute.
* Removed outbox and replaced inbox with :attr:`Actor.mailbox`.
* windowsservice wrapper handle pulsar command lines options.
* Modified the WsgiResponse handling of streamed content.
* Tests can be run in python 2.6 if ``unittest2`` package is installed.
* Fixed chunked transfer encoding.
* Fixed critical bug in socket server :class:`pulsar.Mailbox`. Each client connections
  has its own buffer.
* **71 regression tests**

Ver. 0.2.0 - 2011-Nov-05
=======================================
* A more stable pre-alpha release with overall code refactoring and a lot
  more documentation.
* Fully asynchronous applications.
* Complete re-design of :mod:`pulsar.apps.test` application.
* Added :class:`pulsar.Mailbox` classes for handling message passing between actors.
* Added :mod:`pulsar.apps.ws`, an asynchronous websocket application for pulsar.
* Created the :mod:`pulsar.net` module for internet primitive.
* Added a wrapper class for using pulsar with windows services.
* Removed the `pulsar.worker` module.
* Moved `http.rpc` module to `apps`.
* Introduced context manager for `pulsar.apps.tasks` to handle logs and exceptions.
* **61 regression tests**

Ver. 0.1.0 - 2011-Aug-24
=======================================

* First (very) pre-alpha release.
* Working for python 2.6 and up, including python 3.
* Five different applications: HTTP server, RPC server, distributed task queue,
  asynchronous test suite and asynchronous shell.
* **35 regression tests**

.. _psutil: http://code.google.com/p/psutil/
.. _pypy: http://pypy.org/