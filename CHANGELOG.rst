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
