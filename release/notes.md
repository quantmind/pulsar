## Api

* Pulsar Protocol requires loop as first parameter during initialisation
* Actor uses event loop ``set_debug`` method when running with the ``--debug`` flag

## Internals

* Use ``actor_stop`` rather than ``loop.stop`` when handling OS signals which
  kill an actor.
* Better ``close`` method for ``TCPServer``
* Close the http connection when keep-alive is not available and status code is not 101. Previously the connection was detached only.

## Bug Fixes

* Bug fix in ``HttpRedirect.location`` attribute


## CI

* sudoless testing in travis
