Version Master
=======================================
* Overall code refactoring and a lot more documentation.
* Complete redisign of :mod:`pulsar.apps.test` application.
* Added :class:`pulsar.Mailbox` classes for handling message passing between actors.
* Added :mod:`pulsar.apps.ws`, an asynchronous websocket application for pulsar.
* Asynchronous applications.
* Create the :mod:`pulsar.net` module for stream primitive.
* Added a windows service wrapper class for using pulsar with windows services.
* Removed the `worker` module.
* Moved `http.rpc` module to `apps`.
* Introduced context manager for `apps.tasks` to handle logs and exceptions.
* **37 regression tests**

Version 0.1.0 - 2001-Aug-24
=======================================

* First (very) alpha release.
* Working for python 2.6 and up, including python 3.
* Five different applications: HTTP server, RPC server, distributed task queue,
  asynchronous test suite and asynchronous shell.
* **35 regression tests**