Development
=====================
* Much better handling of stopping servers.
* Refactored :class:`pulsar.Deferred` to be more similar to twisted. You
  can add separate callbacks for handling errors.
* The :class:`pulsar.Mailbox` does not derive from :class:`threading.Thread` so
  that the eventloop can be restarted.

Version 0.3 - 2012-May-03
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

Version 0.2.1 - 2011-Dec-18
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

Version 0.2.0 - 2011-Nov-05
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

Version 0.1.0 - 2011-Aug-24
=======================================

* First (very) pre-alpha release.
* Working for python 2.6 and up, including python 3.
* Five different applications: HTTP server, RPC server, distributed task queue,
  asynchronous test suite and asynchronous shell.
* **35 regression tests**

.. _psutil: http://code.google.com/p/psutil/