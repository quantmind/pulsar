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
   

Server State
==================

Pulsar can be used as a stand-alone asynchronous library, without using
:ref:`actors <design-actor>` to provide parallel execution.
When using :ref:`pulsar application framework <design-application>`
you need to use pulsar in **server state**, that is to say, there
will be a centralised :class:`Arbiter` controlling the main
:class:`EventLoop` in the **main thread** of the **master process**.
The arbiter is a specialised :ref:`IO actor <iobound>`
which control the life all :class:`Actor` and :class:`Monitor`.

.. _design-arbiter:

Arbiter
~~~~~~~~~~~~~~~~~~~~~~~~
To access the arbiter, from the main process, one can use::

    arbiter = pulsar.arbiter()
    
The Arbiter can be stopped and restarted.

.. _design-actor:

Actors
=================

Actors are the atoms of pulsar's concurrent computation, they do not share
state between them, communication is achieved via asynchronous
inter-process message passing, implemented using the standard
python socket library. An :class:`Actor` can be **thread-based** or
**process-based** (default) and control at least one running :class:`EventLoop`.
To obtain the actor in the current thread::

    actor = pulsar.get_actor()
    
Spawning a new actor can be achieved by the :func:`spawn` function, for example
the following will spawn a thread-based actor::

    ap = spawn(concurrency='thread')
    
    
.. _eventloop:

Event loop
~~~~~~~~~~~~~~~
Each actor has its own :attr:`Actor.requestloop`, an instance of :class:`EventLoop`,
which can be used to register handlers on file descriptors.
The :attr:`Actor.requestloop` is initiated just after forking (or after the
actor's thread starts for thread-based actors).
Pulsar :class:`EventLoop` will be following pep-3156_ guidelines.
In addition to the :attr:`Actor.requestloop`, :ref:`cpu bound <cpubound>`
actors have another :class:`EventLoop`, on a different thread, for
handling IO requests on their :ref:`mailbox <actor-mailbox>`.

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


.. _design-mailbox:

Mailbox
~~~~~~~~~~~~~~
Each actor, with the only exception of :class:`Monitor`, have its own
:attr:`Actor.mailbox`, an asynchronous socket server which listen for
messages from other actors. In other words, each actor has an associated
**address**.


.. _design-spawning:

Spawning
~~~~~~~~~~~~~~
Spawning a new actor is achieved via the :func:`spawn` function::
    
    from pulsar import spawn
    
    def periodic_task():
        # do something useful here
        ...
        
    ap = spawn(on_start=lambda: get_event_loop().call_repeatedly(2, periodic_task))
    
The valued returned by :func:`spawn` is an :class:`ActorProxyDeferred` instance,
a specialised :class:`Deferred`, which has the spawned actor id ``aid`` and
it is called back once the remote actor has started.
The callback will be an :class:`ActorProxy`, a lightweight proxy
for the remote actor.

When spawning from an actor other than the :ref:`arbiter <design-arbiter>`,
the workflow of the :func:`spawn` function is as follow:

* :func:`send` a message to the :ref:`arbiter <design-arbiter>` to spawn
  a new actor.
* The arbiter spawn the actor and wait for the actor's **hand shake**. Once the
  hand shake is done, it sends the response (the :class:`ActorProxy` of the
  spawned actor) to the original actor.
        
The actor **hand shake** is the mechanism with which a :class:`Actor` register
its :ref:`mailbox address <design-mailbox>` with the :class:`Arbiter` so that
the arbiter can monitor its behavior. If the hand-shake fails, the spawned
actor will eventually stop.


.. _actor-callbacks:

Hooks
~~~~~~~~~~~~~~~~~~~

An :class:`Actor` exposes three functions which can be
used to customise its behaviour. These functions do nothing in the
standard :class:`Actor` implementation. 

**on_start**

The :meth:`Actor.on_start` method is called, **once only**, just before the actor
starts its :ref:`event loop <eventloop>`. This function can be used to setup
the application and register event handlers. For example, the
:ref:`socket server application <apps-socket>` creates the server and register
its file descriptor with the :attr:`Actor.requestloop` via the
:meth:`IOLoop.add_handler` method.

 
**on_stop**

The :meth:`Actor.on_stop` method is called, **once only**, just before the
actor is garbage collected.
 
**on_info**

The :meth:`Actor.on_info` method is called to provide information about
the actor.


.. _actor_commands:

Commands
~~~~~~~~~~~~~~~~~

An :class:`Actor` communicate with a remote :class:`Actor` by *sending* an
**action** to perform. This action takes the form of a **command** name and
optional positional and key-valued parameters. It is possible to add new
commands via the :class:`pulsar.command` decorator as explained in the
:ref:`api documentation <api-remote_commands>`.


**ping**

Ping the remote actor *abcd* and receive an asynchronous ``pong``::

    send('abcd', 'ping')


**echo**

received an asynchronous echo from a remote actor *abcd*::

    send('abcd', 'echo', 'Hello!')


**run**

Run a function on a remote actor. The function must accept actor as its initial parameter::

    def dosomething(actor, *args, **kwargs):
        ...
    
    send('arbiter', 'run', dosomething, *args, **kwargs)
    
    

.. _design-application:

Application Framework
=============================

To aid the development of applications running on top of pulsar concurrent
framework, the library ships with the :class:`Application` class.



.. _pep-3156: http://www.python.org/dev/peps/pep-3156/