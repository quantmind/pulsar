.. _design:

.. module:: pulsar

=====================
Design
=====================

Pulsar is an asynchronous concurrent framework for python. It implements
a double layer of components for building a vast array of parallel and asynchronous
applications.

* The first layer is based on the building blocks of pulsar library:
  the :class:`Actor` class for concurrency and the the :class:`Deferred` class
  for handling asynchronous events.
* The second layer, built on top of the first one, is based on the
  :class:`Application` class.
   

Server Model
==================

When running as server, the main thread in the master process
is managed by the :class:`Arbiter`, a specialised :ref:`IO actor <iobound>`
which control the life all :class:`Actor` and :class:`Monitor`.


Actors
=================

Actors are the atoms of pulsar’s concurrent computation, they do not share
state between them, communication is achieved via asynchronous
inter-process message passing, implemented using the standard
python socket library. An :class:`Actor` can be **thread-based** or
**process-based** (default) and control at least one running eventloop. To
obtain the actor in the current context::

    actor = pulsar.get_actor()

.. _eventloop:

Event loop
~~~~~~~~~~~~~~~
Each actor has its own :attr:`Actor.requestloop`, an instance of :class:`IOLoop`,
which can be used to register handlers on file descriptors.
The :attr:`Actor.requestloop` is initiated just after forking.
Pulsar event loop will be following pep-3156_ guidelines.

.. _iobound:

IO-bound
~~~~~~~~~~~~~~~
The most common :class:`Actor` has a :meth:`Actor.requestloop` which tells
the operating system (through `epoll` or `select`) that it should be notified
when a new connection is made, and then it goes to sleep.
Serving the new request should occur as fast as possible so that other
connections can be served simultaneously. 

.. _cpubound:

CPU-bound
~~~~~~~~~~~~~~~
The second type of :class:`Actor` can be used to perform CPU intensive
operations, such as calculations, data manipulation or whatever you need
them to do. CPU-bound :class:`Actors` have the following properties:

* Their :attr:`Actor.requestloop` listen for requests on distributed queue
  rather than from a socket.
* Once they receive a new requests, they can block their request loop
  for a long time. 
* In addition to their request loop, they have an I/O event loop running on a
  separate thread. It is accessed via the :meth:`Actor.ioloop` attribute.


.. _actor-callbacks:

Actor Hooks
====================

An :class:`Actor` exposes five functions which can be
used to customise its behaviour. These functions do nothing in the
standard :class:`Actor` implementation. 

on_start
~~~~~~~~~~~~~~~
The :meth:`Actor.on_start` method is called, **once only**, just before the actor
starts its :ref:`event loop <eventloop>`. This function can be used to setup
the application and register event handlers. For example, the
:ref:`socket server application <apps-socket>` creates the server and register
its file descriptor with the :attr:`Actor.requestloop` via the :meth:`IOLoop.add_handler` method.

on_event
~~~~~~~~~~~~~~~
The :meth:`Actor.on_event` method is called when an event on a registered
file descriptor occurs.
 
on_stop
~~~~~~~~~~~~~~~
The :meth:`Actor.on_stop` method is called, **once only**, just before the
actor starts shutting down its event loop.
 
on_exit
~~~~~~~~~~~~~~~
The :meth:`Actor.on_exit` method is called, **once only**, just before the
actor is garbage collected.
 
on_info
~~~~~~~~~~~~~~~
The :meth:`Actor.on_info` method is called to provide information about
the actor.


.. _actor_commands:

Actor commands
========================

An :class:`Actor` communicate with a remote :class:`Actor` by *sending* an
**action** to perform. This action takes the form of a **command** name and
optional positional and key-valued parameters. It is possible to add new
commands via the :class:`pulsar.command` decorator as explained in the
:ref:`api documentation <api-remote_commands>`.


ping
~~~~~~~

Ping the remote actor *abcd* and receive an asynchronous ``pong``::

    send('abcd', 'ping')


echo
~~~~~~~

received an asynchronous echo from a remote actor *abcd*::

    send('abcd', 'echo', 'Hello!')


run
~~~~~~~

Run a function on a remote actor. The function must accept actor as its initial parameter::

    def dosomething(actor, *args, **kwargs):
        ...
    
    send('arbiter', 'run', dosomething, *args, **kwargs)
    
    
.. _application-framework:

Application Framework
=============================

To aid the development of applications running on top of pulsar concurrent
framework, the library ships with the :class:`Application` class.



.. _pep-3156: http://www.python.org/dev/peps/pep-3156/