Development
===========================
* Test WSGI environment does not use asynchronous stream
* Bug fixes in pulsar data store commands
* Critical bug fix in Wsgi Router degault parameters (RouterParameter).
* Increased test coverage
* Code cleanup

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
