## Api

* Pulsar Protocol requires loop as first parameter during initialisation
* Actor uses event loop ``set_debug`` method when running with the ``--debug`` flag
* ``GreenWSGI`` handler moved to ``pulsar.apps.greenio.wsgi``

## Internals

* Use ``actor_stop`` rather than ``loop.stop`` when handling OS signals which
  kill an actor.
* Better ``close`` method for ``TCPServer``
  
## Bug Fixes

* Bug fix in ``HttpRedirect.location`` attribute


## CI

* sudoless testing in travis
