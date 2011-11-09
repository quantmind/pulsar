Version 0.2.1 - 2011-Nov-09
=======================================
* Modified the WsgiResponse handling of streamed content.
* Tests can be run in python 2.6 if ``unittest2`` package is installed.
* Fixed chunked transfer encoding.
* Fixed critical bug in socket server :class:`pulsar.Mailbox`. Each client connections
  has its own buffer.
* **69 regression tests**

Version 0.2.0 - 2011-Nov-05
=======================================
* Overall code refactoring and a lot more documentation.
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

* First (very) alpha release.
* Working for python 2.6 and up, including python 3.
* Five different applications: HTTP server, RPC server, distributed task queue,
  asynchronous test suite and asynchronous shell.
* **35 regression tests**