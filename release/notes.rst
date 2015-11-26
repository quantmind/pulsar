Api
-------------
* Pulsar Protocol requires loop as first parameter during initialisation
* Actor uses event loop ``set_debug`` method when running with the ``--debug`` flag
* Added the release application for making releases. Used by pulsar and other packages.

Internals
-------------
* Use ``actor_stop`` rather than ``loop.stop`` when handling OS signals which
  kill an actor.
* Better ``close`` method for ``TCPServer``
* sudoless testing in travis

Bug Fixes
-------------
* Bug fix in ``HttpRedirect.location`` attribute
