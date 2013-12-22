Ver. 0.7.4 - 2013-Dec-22
===========================
* A bug fix release.
* Fixes an issue with Cookie handling in the wsgi application.
* Don't log errors when writing back to a stale client
* **822 regression tests**, **91% coverage**

Ver. 0.7.3 - 2013-Dec-12
===========================
* A bug fix release.
* ``setup.py`` only import pulsar version and skip the rest
* The :func:`.wait_for_body_middleware` read the HTTP body only without
  decoding it
* C extensions included in ``MANIFEST.in`` so that they can be compiled from PyPi
* **823 regression tests**, **91% coverage**

Ver. 0.7.2 - 2013-Oct-16
===========================
* A bug fix release.
* Must upgrade if using the :ref:`django pulse <apps-pulse>` application.
* Use ujson_ if installed.
* Fixed :ref:`wait for body middleware <wait-for-body-middleware>`.
* Fixed :ref:`django pulse <apps-pulse>` application when the client request
  has body to load.
* **821 regression tests**, **91% coverage**.

Ver. 0.7.1 - 2013-Oct-14
===========================
* Documentation fixes
* Critical fix in ``setup.py`` for python 2.
* Replaced the favicon in documentation.
* **807 regression tests**, **90% coverage**.

Ver. 0.7.0 - 2013-Oct-13
===========================
* Several improvements and bug fixes in the :ref:`Http Client <apps-http>`
  including:
    * SSL support
    * Proxy and Tunneling
    * Cookie support
    * File upload

* Code coverage can be turned on by using the ``--coverage`` option. By
  passing in the command line ``--coveralls`` when testing, coverage is
  published to coveralls.io.
* WSGI responses 400 Bad Request to request with no ``Host`` header if the
  request URI is not an absolute URI. Follows the `rfc2616 sec 5.2`_
  guidelines.
* A new asynchronous :ref:`redis client <redis-client>`. Requires redis-py_.
* Removed the specialised application worker and monitor classes.
  Use standard actor and monitor with specialised
  :ref:`start hooks <actor-hooks>` instead.
* Removed the global event dispatcher. No longer used. Less global variables
  the better.
* Protocol consumer to handle one request only. Better upgrade method for
  connections.
* Proper handling of secure connections in :ref:`wsgi applications <apps-wsgi>`.
* Added ``accept_content_type`` method to :ref:`WSGI Router <wsgi-router>`.
* Ability to add embedded css rules into the :ref:`head <wsgi-html-head>`
  element of an :ref:`Html document <wsgi-html-document>`.
* Added :class:`pulsar.Actor.stream` attribute to write messages without using
  the logger.
* Pass pep8 test.
* **807 regression tests**, **90% coverage**.

.. _`rfc2616 sec 5.2`: http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5.2

Ver. 0.6.0 - 2013-Sep-05
===========================
* Several new features, critical bug fixes and increased tests coverage.
* Asynchronous framework:
    * Removed ``is_async`` function. Not used.
    * The :class:`pulsar.async` decorator always return a
      :class:`pulsar.Deferred`, it never throws.
    * Created the :class:`pulsar.Poller` base class for implementing different
      types of event loop pollers. Implementation available for ``epoll``,
      ``kqueue`` and ``select``.
    * Modified :class:`pulsar.Failure` implementation to handle one ``exc_info``
      only and better handling of unlogged failures.
    * Added an asynchronous FIFO :class:`pulsar.Queue`.
    * Added :func:`pulsar.async_while` utility function.
    * Socket servers handle IPV6 addresses.
    * Added :ref:`SSL support <socket-server-ssl>` for socket servers.
    * Tasks throw errors back to the coroutine via the generator ``throw``
      method.
    * 50% Faster :class:`pulsar.Deferred` initialisation.
    * Added :meth:`pulsar.Deferred.then` method for adding a deferred to a
      deferred's callbacks without affecting the result.

* Actors:
    * Added :ref:`--thread_workers <setting-thread_workers>` config option
      for controlling the default number of workers in actor thread pools.
    * New asynchronous :class:`pulsar.ThreadPool` for CPU bound operations.
    * :ref:`Actor's hooks can be asynchronous <actor-hooks>`.

* Applications:
    * Added ``flush`` method to the
      :ref:`task queue backend <apps-taskqueue-backend>`.
      The metod can be used to remove all tasks and empty the task queue.
    * Better handling of :ref:`non-overlapping jobs <job-non-overlap>`
      in a task queue.
    * Added :ref:`when_exit <setting-when_exit>` application hook.
    * Added :ref:`--io option <setting-poller>` for controlling the default
      :class:`pulsar.Poller`.
    * Critical bug fix in python 3 WSGI server.
    * Added ``full_route`` and ``rule`` attributes to wsgi Router.
    * Added :ref:`--show_leaks option <setting-show_leaks>`
      for showing a memory leak report after a test run.
    * Added :ref:`-e, --exclude-labels option <setting-exclude_labels>`
      for excluding labels in a test run.
    * Several fixes in the test application.
    * Critical bug fix in python Http parser (4bd8a54_).
    * Bug fix and enhancement of :ref:`Router <wsgi-router>` metaclass. It
      is now possible to overwrite the relative ``position`` of children routes
      via the :ref:`route decorator <wsgi-route-decorator>`.

* Examples:
    * Proxy server example uses the new :class:`pulsar.Queue`.

* Miscellaneous:
    * Added :mod:`pulsar.utils.exceptions` documentation.

* **558 regression tests**, **88% coverage**.

.. _4bd8a54: https://github.com/quantmind/pulsar/commit/4bd8a540c4cb7887b65e409fa0f61a36a29590dc

Ver. 0.5.2 - 2013-June-30
==============================
* Introduced the :ref:`Router parameter <tutorial-router>` for propagating
  attributes to children routes. router can also have a ``name`` so that
  they can easily be retrieved via the ``get_route`` method.
* Bug fix in Asynchronous Wsgi String ``__repr__`` method.
* Critical bug fix in Wsgi server when a failure without a stack trace occurs.
* Critical bug fix in WebSocket frame parser.
* WebSocket handlers accept the WebSocket protocol as first argument.
* **448 regression tests**, **87% coverage**.

Ver. 0.5.1 - 2013-June-03
==============================
* Several bug fixes and more docs.
* Fixed ``ThreadPool`` for for python 2.6.
* Added the :func:`pulsar.safe_async` function for safely executing synchronous
  and asynchronous callables.
* The :meth:`pulsar.utils.config.Config.get` method never fails. It return the
  ``default`` value if the setting key is not available.
* Improved ``setup.py`` so that it does not log a python 2 module syntax error
  when installing for python 3.
* :ref:`Wsgi Router <wsgi-router>` makes sure that the ``pulsar.cache`` key in
  the ``environ`` does not contain asynchronous data before invoking the
  callable serving the request.
* **443 regression tests**, **87% coverage**.

Ver. 0.5.0 - 2013-May-22
==============================
* This is a major release with considerable amount of internal refactoring.
* Asynchronous framework:
   * pep-3156_ implementation.
   * New pep-3156_ compatible :class:`pulsar.EventLoop`.
   * Added the :meth:`pulsar.Deferred.cancel` method to cancel asynchronous
     callbacks.
   * :class:`pulsar.Deferred` accepts a *timeout* as initialisation parameter.
     If a value greater than 0 is given, the deferred will add a timeout to the
     event loop to cancel itself in *timeout* seconds.
   * :class:`pulsar.Task` stops after the first error by default.
     This class replace the old DeferredGenerator and provides a cleaner
     API with inline syntax. Check the
     :ref:`asynchronous components <tutorials-coroutine>` tutorial for
     further information.
   * Added :func:`pulsar.async_sleep` function.

* Actors:
   * :class:`pulsar.Actor` internal message passing uses the (unmasked)
     websocket protocol in a bidirectional communication between the
     :class:`pulsar.Arbiter` and actors.
   * Spawning and stopping actors is monitored using a timeout set at 5 seconds.
   * Added :mod:`pulsar.async.consts` module for low level pulsar constants.
   * Removed the requestloop attribute, the actor event loop is now accessed
     via the :attr:`pulsar.Actor.event_loop` attribute or via the pep-3156_
     function ``get_event_loop``.

* Applications:
    * Added ability to add Websocket sub-protocols and extensions.
    * New asynchronous :class:`pulsar.apps.http.HttpClient` with websocket
      support.
    * Support http-parser_ for faster http protocol parsing.
    * Refactoring of asynchronous :mod:`pulsar.apps.test` application.
    * Added :ref:`Publish/Subscribe application <apps-pubsub>`. The application
      is used in the :ref:`web chat <tutorials-chat>` example.
    * Added :ref:`django application <apps-pulse>` for running a django_
      site using pulsar.
    * :func:`pulsar.apps.get_application` returns a :ref:`coroutine <coroutine>`
      so that it can be used in any process domain.

* Initial :ref:`twisted integration <tutorials-twisted>`.
   * Added :func:`pulsar.set_async` function which can be used to change
     the asynchronous discovery functions :func:`pulsar.maybe_async`
     and :func:`pulsar.maybe_failure`. The function is used in the
     implementation of :ref:`twisted integration <tutorials-twisted>` and could
     be used in conjunction with other asynchronous libraries as well.
   * New :ref:`Webmail example application <tutorials-webmail>` using twisted
     IMAP4 protocol implementation.
* Added :mod:`pulsar.utils.structures.FrozenDict`.
* **444 regression tests**, **87% coverage**.

Ver. 0.4.6 - 2013-Feb-8
==============================
* Added websocket chat example.
* Fixed bug in wsgi parser.
* Log WSGI environ on HTTP response errors.
* Several bug-fixes in tasks application.
* **374 regression tests**, **87% coverage**.

Ver. 0.4.5 - 2013-Jan-27
==============================
* Refactored :class:`pulsar.apps.rpc.JsonProxy` class.
* Websocket does not support any extensions by default.
* **374 regression tests**, **87% coverage**.

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
.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
.. _http-parser: https://github.com/benoitc/http-parser
.. _django: https://www.djangoproject.com/
.. _redis: http://redis.io/
.. _redis-py: https://github.com/andymccurdy/redis-py
.. _ujson: https://pypi.python.org/pypi/ujson
