Changelog Pre Pulsar 1.0
============================

Ver. 0.9.2 - 2014-Nov-18
---------------------------------------------------
* A minor release with several bug fixes.
* Bug fix in :class:`.Links`
* A more flexible implementation of the :meth:`~.Router.make_router` method
* Added the :meth:`~Router.copy` method for cloning a router class without
  its routes.

Ver. 0.9.1 - 2014-Oct-14
---------------------------------------------------
* Several fixes on :class:`.Html` initialisation and the :meth:`.Html.attr`
  method
* Critical bug fix in :class:`.WsgiHandler` when passing response middleware
* unidecode_ removed as hard dependency, wrong license
* The :ref:`Httpbin <tutorials-httpbin>` example shows how to
  use :ref:`application hooks <setting-section-application-hooks>`
* Added :meth:`.AsyncString.before_render` method to add callbacks executed
  just before an asynchronous string is rendered
* Fixed :ref:`--daemon <setting-daemon>` mode

Ver. 0.9.0 - 2014-Aug-04
---------------------------------------------------
* Works with trollius_ 1.0.1 which is now a dependency for all supported
  python versions
* Added :class:`.FlowControl` and :class:`.Timeout` protocol mixins,
  :class:`.PulsarProtocol` now inherits from :class:`.FlowControl`
* Better handling of streaming responses by the wsgi server
* Added the :ref:`--reload <setting-reload>` command line option and
  configuration parameter. If used, pulsar auto-reload code changes (useful
  during development)
* Added ``file`` log handler
* Javascript and Css defaults libraries moved to a new project
  (https://github.com/quantmind/jslibs) and the json file
  (http://quantmind.github.io/jslibs/libs.json) lazily loaded if needed by
  either :class:`.Links` or :class:`.Scripts` classes
* The :class:`.HttpClient` can be used in conjunction with
  :ref:`greenlet support <green-http>` to write implicit asynchronous HTTP
  requests
* Finally removed the ``get_request_loop`` method. Full compatibility with
  asyncio
* Bug fix in :class:`.Store` constructor
* When throwing an :class:`.ImproperlyConfigured` exception, pulsar will log
  and error without the full stack-trace
  (:class:`~.ImproperlyConfigured.exit_code` attribute is set to 2).
  Useful when stopping execution because of a wrong input rather than an
  internal exception
* Critical bug fix in :class:`.Router` when children are added via a decorated
  method
* trollius_ and unidecode_ added as dependencies in the ``setup.py``
  script during installation
* Better :func:`.slugify` function
* :class:`.EventHandler` requires a valid :ref:`event loop <asyncio-event-loop>`
  during initialisation
* Removed ``arbiter`` and ``monitor`` modules from ``async``, one :class:`.Actor`
  class only, implementation differences handled by the different underlying
  :class:`.Concurrency` classes.
* The :class:`.Config` adds the default values of excluded settings to the
  :attr:`.Config.params` dictionary. In this way the parameters cannot be set
  on the command line but still be available.


Ver. 0.8.4 - 2014-Jul-07
---------------------------------------------------
* Several bug fixes in wsgi :class:`.Router`
* Added :attr:`.Route.name` attribute
* :class:`.WsgiResponse` does not send cookies back to the client by default
* Critical bug fix for multiprocessing sockets when running on python 3.4 and
  windows

Ver. 0.8.3 - 2014-Jun-23
---------------------------------------------------
* Fixed critical bug in python 2 for :func:`.middleware_in_executor`
* Set trollius logger to warning by default (same as asyncio)
* Added :meth:`.WsgiRequest.redirect` and :meth:`.Router.redirect` methods
  to simplify redirection.
* Renamed css container as :class:`.Links`

Ver. 0.8.2 - 2014-May-30
---------------------------------------------------
* :class:`.ProtocolConsumer` has its own ``_loop`` attribute rather than
  obtaining indirectly from the underling :class:`.Connection`.
  This avoids several logging errors when a connection is dropped
* Added utilities to execute :postgresql:`PostgreSql <>` queries via
  psycopg2_ in asynchronous mode via the :mod:`~pulsar.apps.greenio` module.
* :ref:`Django pulse application <apps-pulse>` can be run asynchronously
  when using PostgreSql database.
  It requires the :greenlet:`greenlet module <>`
* Added :attr:`.Head.embedded_js` for adding javascript code directly in the
  :class:`.HtmlDocument`
* Improved management of ``meta`` tags in the HTML5 :class:`.Head` class
* Added :class:`.OAuth1` and :class:`.OAuth2` hooks to the
  :mod:`~pulsar.apps.http` module (alpha and untested)
* Bug fix in :class:`.HttpParser` when ``Transfer-Encoding=chunked``
* Added default javascript libraries to the :class:`.HtmlDocument`
* Both wsgi request wrappers and content don't use coroutines but
  straight :class:`~asyncio.Future` for compatibility with other frameworks.
* pulsar can be imported and used (with limited scope) in the google appengine

Ver. 0.8.1 - 2014-Apr-14
---------------------------------------------------
* Added :mod:`pulsar.apps.greenio` application for writing asynchronous code
  using the greenlet_ library.
* Moved :class:`.PulsarDS` server into its own :mod:`pulsar.apps.ds`
  module
* The task application can run on redis.
* Added twisted integration (alpha)
* Removed ``Server`` and ``Date`` from Hop headers
* Fixed installation problem with extensions
* More documentation for data stores
* Added ability to serve directories in :class:`.MediaRouter` if the
  path contain a ``default_file`` (``index.html``). This also means
  ``show_indexes`` in :class:`.MediaRouter` initialisation is by default
  ``False``.
* The callable method in a :class:`.AsyncString` always returns a
  :class:`~asyncio.Future`.

Ver. 0.8.0 - 2014-Mar-06
---------------------------------------------------
* **Backward incompatible version**

* **Asyncio Integration**

  * asyncio_ integration with several changes in internals. The integration
    works with all supported python versions: 2.7, 3.3 and 3.4
  * Asyncio event loop functions :func:`~asyncio.get_event_loop`,
    :func:`~asyncio.new_event_loop`,
    are available from pulsar top level module as well as asyncio.
    In other words ``from pulsar import get_event_loop`` and
    ``from asyncio import get_event_loop`` are equivalent (provided pulsar is
    imported first).
  * Replaced the ``Deferred`` class with :class:`asyncio.Future`.
  * Replaced the ``EventLoop`` class with
    :ref:`asyncio event loop <asyncio-event-loop>`.

* **Core library**

  * Removed support for python 2.6 and python 3.2.
  * :ref:`Coroutines <coroutine>` can return a value via the
    :func:`.coroutine_return` function.
  * Added :func:`.run_in_loop` high level function. This utility
    runs a callable in the event loop thread and returns a
    :class:`~asyncio.Future` called back once the callable has
    a result/exception.
  * Added :func:`.in_loop` and :func:`.task` decorators for
    member functions of :ref:`async objects <async-object>`.
  * :func:`.async` is now a function, not a decorator.
  * Added the new :class:`.Pool` class for managing a pool of asynchronous
    connection with a server.
  * Embedding third-party asynchronous frameworks can be achieved via the
    new :func:`.add_async_binding` function.
  * Removed ``Client`` class and replaced by :class:`.AbstractClient` which
    in turns is a subclass of connections :class:`.Producer`.
  * Removed ``force_sync`` parameter when creating synchronous components.
    Synchronous objects are now created by explicitly passing a new event
    loop during initialisation.
    Check the the :ref:`synchronous components tutorial <tutorials-synchronous>`
    for details.
  * Added the :ref:`data-store <setting-data_store>` setting for specifying
    the default data store of a running application.
  * Added the :ref:`exc-id <setting-exc_id>` setting which uniquely specify
    the identity of a running application. This is useful during testing.
  * Unified the handshake across all actors

* **New data store module**

  * New :mod:`pulsar.apps.data` module for managing asynchronous data stores.
  * Two stores available: redis_ and :ref:`pulsar-ds <pulsar-data-store>`.
  * Additional stores can be created by subclassing the :class:`.Store`
    abstract class and registering it via the :func:`.register_store` function.
  * The :ref:`pulsar-ds <pulsar-data-store>` is a python implementation of
    the popular redis server. It implements most redis commands including
    scripting.

* **Websockets**

  * The web socket :meth:`~pulsar.apps.ws.WS.on_open` method is invoked soon
    after upgrade headers are sent. No need to send a message from the client
    to kick start the bidirectional communication.
  * Websocket C extensions for faster parsing/masking.
  * Added support for sending :meth:`~pulsar.utils.websocket.FrameParser.close`
    frames with an optional status code, and for parsing close frames
    with a body via the :func:`.parse_close` function (for websocket clients).

* **WSGI**

  * Better handling of cookies in :class:`.WsgiResponse`
  * :class:`.Router` can have children even if it is a leaf node
  * Dropped support for http-parser_, only HTTP python parser used

* **Miscellaneous**

  * The :mod:`pulsar.apps.pubsub` has been removed. Publish/subscribe
    implementations are now available in the new :mod:`pulsar.apps.data` module.
  * The ``Backend`` class has been removed.
  * Improved :ref:`django example <tutorials-django>` with possibility to
    choose different data stores for messages.
  * Removed the twisted integration module and moved it to the example directory.
    The integration is not tested enough and therefore cannot be part of the
    main distribution.
  * :class:`.Application` does not require to create picklable objects.
  * More robust serialisation of ``TestCase`` methods when used with the
    :func:`.run_on_arbiter` decorator.
  * The shell application runs on a worker thread in the
    arbiter domain.
  * The :meth:`.Configurator.start` method returns a :class:`~asyncio.Future`
    called back once the applications in the configurator are running.
  * Added a new script for building releases for pypi (``buildrelease.py``)

* **1,166 regression tests**, **91% coverage**.

Ver. 0.7.4 - 2013-Dec-22
---------------------------------------------------
* A bug fix release.
* Fixes an issue with Cookie handling in the wsgi application.
* Don't log errors when writing back to a stale client
* **822 regression tests**, **91% coverage**

Ver. 0.7.3 - 2013-Dec-12
---------------------------------------------------
* A bug fix release.
* ``setup.py`` only import pulsar version and skip the rest
* The :func:`.wait_for_body_middleware` read the HTTP body only without
  decoding it
* C extensions included in ``MANIFEST.in`` so that they can be compiled from PyPi
* **823 regression tests**, **91% coverage**

Ver. 0.7.2 - 2013-Oct-16
---------------------------------------------------
* A bug fix release.
* Must upgrade if using the :ref:`django pulse <apps-pulse>` application.
* Use ujson_ if installed.
* Fixed :ref:`wait for body middleware <wait-for-body-middleware>`.
* Fixed :ref:`django pulse <apps-pulse>` application when the client request
  has body to load.
* **821 regression tests**, **91% coverage**.

Ver. 0.7.1 - 2013-Oct-14
---------------------------------------------------
* Documentation fixes
* Critical fix in ``setup.py`` for python 2.
* Replaced the favicon in documentation.
* **807 regression tests**, **90% coverage**.

Ver. 0.7.0 - 2013-Oct-13
---------------------------------------------------
* Several improvements and bug fixes in the :ref:`Http Client <apps-http>`
  including:
  * SSL support
  * Proxy and Tunnelling
  * Cookie support
  * File upload

* Code coverage can be turned on by using the ``--coverage`` option. By
  passing in the command line ``--coveralls`` when testing, coverage is
  published to coveralls.io.
* WSGI responses 400 Bad Request to request with no ``Host`` header if the
  request URI is not an absolute URI. Follows the `rfc2616 sec 5.2`_
  guidelines.
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
* Added :class:`.Actor.stream` attribute to write messages without using
  the logger.
* Pass pep8 test.
* **807 regression tests**, **90% coverage**.

.. _`rfc2616 sec 5.2`: http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5.2

Ver. 0.6.0 - 2013-Sep-05
---------------------------------------------------
* Several new features, critical bug fixes and increased tests coverage.
* **Core library**:

  * Removed ``is_async`` function. Not used.
  * The :class:`.async` decorator always return a
    :class:`.Deferred`, it never throws.
  * Created the :class:`.Poller` base class for implementing different
    types of event loop pollers. Implementation available for ``epoll``,
    ``kqueue`` and ``select``.
  * Modified :class:`.Failure` implementation to handle one ``exc_info``
    only and better handling of unlogged failures.
  * Added an asynchronous FIFO :class:`.Queue`.
  * Added :func:`.async_while` utility function.
  * Socket servers handle IPV6 addresses.
  * Added :ref:`SSL support <socket-server-ssl>` for socket servers.
  * Tasks throw errors back to the coroutine via the generator ``throw``
    method.
  * 50% Faster :class:`.Deferred` initialisation.
  * Added :meth:`.Deferred.then` method for adding a deferred to a
    deferred's callbacks without affecting the result.

* **Actors**:

  * Added :ref:`--thread_workers <setting-thread_workers>` config option
    for controlling the default number of workers in actor thread pools.
  * New asynchronous :class:`.ThreadPool` for CPU bound operations.
  * :ref:`Actor's hooks can be asynchronous <actor-hooks>`.

* **Applications**:

  * Added ``flush`` method to the task queue backend.
    The metod can be used to remove all tasks and empty the task queue.
  * Better handling of non-overlapping jobs in a task queue.
  * Added :ref:`when_exit <setting-when_exit>` application hook.
  * Added :ref:`--io option <setting-selector>` for controlling the default
    selector from python :mod:`selectors` module.
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

* **Miscellaneous**:

  * Proxy server example uses the new :class:`.Queue`.
  * Added :mod:`~pulsar.utils.exceptions` documentation.

* **558 regression tests**, **88% coverage**.

.. _4bd8a54: https://github.com/quantmind/pulsar/commit/4bd8a540c4cb7887b65e409fa0f61a36a29590dc

Ver. 0.5.2 - 2013-June-30
---------------------------------------------------
* Introduced the Router parameter for propagating
  attributes to children routes. router can also have a ``name`` so that
  they can easily be retrieved via the ``get_route`` method.
* Bug fix in Asynchronous Wsgi String ``__repr__`` method.
* Critical bug fix in Wsgi server when a failure without a stack trace occurs.
* Critical bug fix in WebSocket frame parser.
* WebSocket handlers accept the WebSocket protocol as first argument.
* **448 regression tests**, **87% coverage**.

Ver. 0.5.1 - 2013-June-03
---------------------------------------------------
* Several bug fixes and more docs.
* Fixed ``ThreadPool`` for for python 2.6.
* Added the :func:`.safe_async` function for safely executing synchronous
  and asynchronous callables.
* The :meth:`.Config.get` method never fails. It return the
  ``default`` value if the setting key is not available.
* Improved ``setup.py`` so that it does not log a python 2 module syntax error
  when installing for python 3.
* :ref:`Wsgi Router <wsgi-router>` makes sure that the ``pulsar.cache`` key in
  the ``environ`` does not contain asynchronous data before invoking the
  callable serving the request.
* **443 regression tests**, **87% coverage**.

Ver. 0.5.0 - 2013-May-22
---------------------------------------------------
* This is a major release with considerable amount of internal refactoring.
* **Core library**

  * pep-3156_ implementation.
  * New pep-3156_ compatible :class:`.EventLoop`.
  * Added the :meth:`.Deferred.cancel` method to cancel asynchronous
    callbacks.
  * :class:`.Deferred` accepts a *timeout* as initialisation parameter.
    If a value greater than 0 is given, the deferred will add a timeout to the
    event loop to cancel itself in *timeout* seconds.
  * :class:`.DeferredTask` stops after the first error by default.
    This class replace the old DeferredGenerator and provides a cleaner
    API with inline syntax. Check the
    :ref:`asynchronous components <tutorials-coroutine>` tutorial for
    further information.
  * Added :func:`.async_sleep` function.

* **Actors**

  * :class:`.Actor` internal message passing uses the (unmasked)
    websocket protocol in a bidirectional communication between the
    :class:`.Arbiter` and actors.
  * Spawning and stopping actors is monitored using a timeout set at 5 seconds.
  * Added :mod:`pulsar.async.consts` module for low level pulsar constants.
  * Removed the requestloop attribute, the actor event loop is now accessed
    via the :attr:`.Actor._loop` attribute or via the pep-3156_
    function ``get_event_loop``.

* **Applications**

  * Added ability to add Websocket sub-protocols and extensions.
  * New asynchronous :class:`.HttpClient` with websocket support.
  * Support http-parser_ for faster http protocol parsing.
  * Refactoring of asynchronous :mod:`pulsar.apps.test` application.
  * Added :ref:`Publish/Subscribe application <apps-pubsub>`. The application
    is used in the :ref:`web chat <tutorials-chat>` example.
  * Added :ref:`django application <apps-pulse>` for running a django_
    site using pulsar.
  * :func:`~pulsar.apps.get_application` returns a :ref:`coroutine <coroutine>`
    so that it can be used in any process domain.

* **Initial twisted integration**

  * Introduced in this application.
  * Added :func:`~.set_async` function which can be used to change
    the asynchronous discovery functions :func:`.maybe_async`
    and :func:`.maybe_failure`. The function is used in the
    implementation of twisted integration and could
    be used in conjunction with other asynchronous libraries as well.
  * New Webmail example application using twisted
    IMAP4 protocol implementation.

* Added :class:`.FrozenDict`.
* **444 regression tests**, **87% coverage**.

Ver. 0.4.6 - 2013-Feb-8
---------------------------------------------------
* Added websocket chat example.
* Fixed bug in wsgi parser.
* Log WSGI environ on HTTP response errors.
* Several bug-fixes in tasks application.
* **374 regression tests**, **87% coverage**.

Ver. 0.4.5 - 2013-Jan-27
---------------------------------------------------
* Refactored :class:`pulsar.apps.rpc.JsonProxy` class.
* Websocket does not support any extensions by default.
* **374 regression tests**, **87% coverage**.

Ver. 0.4.4 - 2013-Jan-13
---------------------------------------------------
* Documentation for development version hosted on github.
* Modified :meth:`.Actor.exit` so that it shuts down :attr:`.Actor.mailbox`
  after closing the :attr:`.Actor.requestloop`.
* Fixed bug which prevented :ref:`daemonisation <setting-daemon>` in posix systems.
* Changed the :meth:`.Deferred.result_or_self` method to return the
  *result* when the it is called and no callbacks are available.
  It avoids several unnecessary calls on deeply nested :class:`.Deferred`
  (which sometimes caused maximum recursion depth exceeded).
* Fixed calculator example script.
* **374 regression tests**, **87% coverage**.

Ver. 0.4.3 - 2012-Dec-28
---------------------------------------------------
* Removed the tasks in event loop. A task can only be added by appending
  callbacks or timeouts.
* Fixed critical bug in :class:`.MultiDeferred`.
* Test suite works with multiple test workers.
* Fixed issue #17 on asynchronous shell application.
* Dining philosophers example works on events only.
* Removed obsolete safe_monitor decorator in :mod:`pulsar.apps`.
* **365 regression tests**, **87% coverage**.

Ver. 0.4.2 - 2012-Dec-12
---------------------------------------------------
* Fixed bug in boolean validation.
* Refactored :class:`.TestPlugin` to handle multi-parameters.
* Removed unused code and increased test coverage.
* **338 regression tests**, **86% coverage**.

Ver. 0.4.1 - 2012-Dec-04
---------------------------------------------------
* Test suite can load test from single files as well as directories.
* :func:`.handle_wsgi_error` accepts optional ``content_type``
  and ``encoding`` parameters.
* Fix issue #20, test plugins not included are not available in the command line.
* :class:`.Application` call :meth:`.Config.on_start` before starting.
* **304 regression tests**, **83% coverage**.

Ver. 0.4 - 2012-Nov-19
---------------------------------------------------
* Overall refactoring of API and therefore incompatible with previous versions.
* Development status set to ``Beta``.
* Support pypy_ and python 3.3.
* Added the new :mod:`pulsar.utils.httpurl` module for HTTP tools and HTTP
  synchronous and asynchronous clients.
* Refactored :class:`.Deferred` to be more compatible with twisted. You
  can add separate callbacks for handling errors.
* Added :class:`.MultiDeferred` for handling a group of asynchronous
  elements independent from each other.
* The :class:`pulsar.Mailbox` does not derive from :class:`threading.Thread` so
  that the eventloop can be restarted.
* Removed the ``ActorMetaClass``. Remote functions are specified using
  a dictionary.
* Socket and WSGI :class:`.Application` are built on top of the new
  ``AsyncSocketServer`` framework class.
* **303 regression tests**, **83% coverage**.

Ver. 0.3 - 2012-May-03
---------------------------------------------------
* Development status set to ``Alpha``.
* This version brings several bug fixes, more tests, more docs, and improvements
  in the :mod:`pulsar.apps.tasks` application.
* Added :meth:`.Job.send_to_queue` method for allowing
  :class:`.Task` to create new tasks.
* The current :class:`.Actor` is always available on the current thread
  ``actor`` attribute.
* Trap errors in :meth:`pulsar.IOLoop.do_loop_tasks` to avoid having monitors
  crashing the arbiter.
* Added :func:`pulsar.system.system_info` function which returns system information
  regarding a running process. It requires psutil_.
* Added global :func:`.spawn` and :func:`.send` functions for
  creating and communicating between :class:`.Actor`.
* Fixed critical bug in :meth:`pulsar.net.HttpResponse.default_headers`.
* Added :meth:`pulsar.utils.http.Headers.pop` method.
* Allow :attr:`pulsar.apps.tasks.Job.can_overlap` to be a callable.
* Added :attr:`pulsar.apps.tasks.Job.doc_syntax` attribute which defaults to
  ``"markdown"``.
* :class:`.Application` can specify a version which overrides
  :attr:`pulsar.__version__`.
* Added Profile test plugin to :ref:`test application <apps-test>`.
* Task scheduler check for expired tasks via the
  :meth:`pulsar.apps.tasks.Task.check_unready_tasks` method.
* PEP 386-compliant version number.
* Setup does not fail when C extensions fail to compile.
* **95 regression tests**, **75% coverage**.

Ver. 0.2.1 - 2011-Dec-18
---------------------------------------------------
* Catch errors in :func:`pulsar.apps.test.run_on_arbiter`.
* Added new setting for configuring http responses when an unhandled error
  occurs (Issue #7).
* It is possible to access the actor :attr:`.Actor.ioloop` form the
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
---------------------------------------------------
* A more stable pre-alpha release with overall code refactoring and a lot
  more documentation.
* Fully asynchronous applications.
* Complete re-design of :mod:`pulsar.apps.test` application.
* Added :class:`.Mailbox` classes for handling message passing between actors.
* Added :mod:`pulsar.apps.ws`, an asynchronous websocket application for pulsar.
* Created the :mod:`pulsar.net` module for internet primitive.
* Added a wrapper class for using pulsar with windows services.
* Removed the `pulsar.worker` module.
* Moved `http.rpc` module to `apps`.
* Introduced context manager for `pulsar.apps.tasks` to handle logs and exceptions.
* **61 regression tests**

Ver. 0.1.0 - 2011-Aug-24
---------------------------------------------------

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
.. _asyncio: http://www.python.org/dev/peps/pep-3156/
.. _cauchdb: http://couchdb.apache.org/
.. _greenlet: http://greenlet.readthedocs.org/
.. _psycopg2: http://pythonhosted.org/psycopg2/
.. _trollius: http://trollius.readthedocs.org/
.. _unidecode: https://pypi.python.org/pypi/Unidecode
