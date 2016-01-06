Ver. 1.1.0 - 2016-Jan-06
============================
* Full support for python 3.5
* Pulsar **1.1.x** is the last minor release ("major.minor.micro") to support python 3.4
* From pulsar **1.2.x**, support for python 3.4 will be dropped and the new
  async-await_ syntax will be used in the whole codebase

Core
-----------------
* Added CI for python 3.5
* Added ``debug`` properties to all ``AsyncObject``. The property returns the event loop
  debug mode flag

HttpClient
----------------
* Backward incompatible changes with API much closer to requests_ and far better support for streaming both uploads and downloads
* Added ``content`` attribute to ``HttpResponse``, in line with requests_
* Ability to pass ``stream=True`` during a request, same API as python requests_
* Added the ``raw`` property to the Http Response, it can be used in conjunction with
  ``stream`` to stream http data. Similar API to requests_
* Renamed ``proxy_info`` to ``proxies``, same API as python requests_
* You can now pass ``proxies`` dictionary during a request
* Stream uploads by passing a generator as ``data`` parameter
* Better websocket upgrade process
* Tidy up ``CONNECT`` request url (for tunneling)
* Added tests for proxyserver example using requests_

WSGI
------
* Both ``wsgi`` and ``http`` apps use the same ``pulsar.utils.httpurl.http_chunks``
  function for transfer-encoding ``chunked``
* ``render_error`` escapes the Exception message to prevent XSS_

Data Store
-----------
* Better ``pulsards_url`` function, default value form ``cfg.data_store``
* ``key_value_save`` set to empty list by default (no persistence)

Examples
-------------
* Refactored proxy server example
* Updated django chat example so that warning are no longer issued

.. _requests: http://docs.python-requests.org/
.. _XSS: https://en.wikipedia.org/wiki/Cross-site_scripting
.. _async-await: https://www.python.org/dev/peps/pep-0492/#specification


Ver. 1.0.7 - 2015-Dec-10
============================
Api
-------------
* Improvements in the ``release`` application
* Handle ``StopIteration`` in green pool [[182](https://github.com/quantmind/pulsar/pull/182)]


Ver. 1.0.6 - 2015-Nov-26
============================
Api
-------------
* Pulsar Protocol requires loop as first parameter during initialisation
* Actor uses event loop ``set_debug`` method when running with the ``--debug`` flag
* ``GreenWSGI`` handler moved to ``pulsar.apps.greenio.wsgi``
* Added the release application for making releases. Used by pulsar and other packages.

Internals
-------------
* Use ``actor_stop`` rather than ``loop.stop`` when handling OS signals which kill an actor.
* Better ``close`` method for ``TCPServer``
* sudoless testing in travis

Bug Fixes
-------------
* Bug fix in ``HttpRedirect.location`` attribute


Ver. 1.0.5 - 2015-Nov-12
===========================
* Asynchronous Redis locking primitive for distributing computing
* Added the :ref:`Twitter Streaming <tutorials-tweets>` tutorial
* Added Javascript directory in examples and a gruntfile for compiling and linting scripts
* Better handling of Ctrl-C in the test application
* Data streaming for ``multipart/form-data`` content type
* Write EOF before closing connections
* Documentation and bug fixes

Ver. 1.0.3 - 2015-Jul-21
===========================
* Flake8 on all codebase
* Added JSON-RPC 2.0 Batch - part of specification (by artemmus)
* Attach configuration ``connection_made`` and ``connection_lost``
  to connections of TCP and UDP servers (connection providers).
* Bug fix in Connection ``data_received`` method. The ``data_processed``
  event was not triggered.
* Process title does not append arbiter to the main process name.
* Added a snippet in examples on how to build a simple framework for remote
  objects.
* Better handling of content-type headers in the HTTP client requests
* Test coverage at 87%

Ver. 1.0.2 - 2015-Jun-16
===========================
* Test WSGI environment does not use asynchronous stream
* Bug fixes in pulsar data store commands
* Critical bug fix in Wsgi Router default parameters (RouterParameter).
* Increased test coverage
* Code cleanup and several internal fixes

Ver. 1.0.1 - 2015-Jun-03
===========================
* Better support of ``get_version`` for third party packages.
* Added optional timeout to :class:`.HttpClient` requests.
* Refactored :class:`.String` and renamed from ``AsyncString``. ``AsyncString``
  still available for backward compatibility.
* Added the new :class:`.GreenLock` class. A locking primitive for
  greenlets in a greenlet pool.
* Added new example to snippets directory. A simple Actor application.

Ver. 1.0.0 - 2015-May-18
===========================

* Python 3.4 and above
* New test runner
* Dropped task application
* Dropped twisted integration
* Dropped data mapper application
* Dropped pulsar shell application
