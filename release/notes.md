## Api

* Pulsar Protocol requires loop as first parameter during initialisation


## Internals

* Use ``actor_stop`` rather than ``loop.stop`` when handling OS signals which
  kill an actor.
* Better ``close`` method for ``TCPServer``.
  

## Bug Fixes

* Bug fix in ``HttpRedirect.location`` attribute


## CI

* sudoless testing in travis
