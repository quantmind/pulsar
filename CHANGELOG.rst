Version Master
=======================================
* The arbiter has its own socket which can be accessed by actors to wake up the
  arbiter polling event loop.
* Asyncronous applications.
* Create the :mod:`net` module for socket and stream primitive.
* Added a windows service wrapper class for using pulsar with windows services.
* Removed the `worker` module.
* Moved `http.rpc` module to `apps`.
* Introduced context manager for `apps.tasks` to handle logs and exceptions.

Version 0.1.0 - 2001-Aug-24
=======================================

* First (very) alpha release.
* Working for python 2.6 and up, including python 3.
* Five different applications: HTTP server, RPC server, distributed task queue,
  asynchronous test suite and asynchronous shell.
* **35 regression tests**