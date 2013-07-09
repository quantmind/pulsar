.. _design:

.. module:: pulsar

=====================
Design
=====================

Pulsar implements a double layer of components for building a vast array
of parallel and asynchronous applications.

* The first layer is based on the building blocks of pulsar library:
  the :class:`Actor` class for parallel execution and the the :class:`Deferred`
  class for handling asynchronous events via an actor's :class:`EventLoop`.
* The second layer, built on top of the first one, is based on the
  :class:`apps.Application` class.
   

Server State
==================

Pulsar can be used as a stand-alone asynchronous library, without using
:ref:`actors <design-actor>` to provide parallel execution. However,
when using :ref:`pulsar application framework <design-application>`,
you need to use pulsar in **server state**, that is to say, there
will be a centralised :class:`Arbiter` controlling the main
:class:`EventLoop` in the **main thread** of the **master process**.
The arbiter is a specialised :class:`Actor`
which control the life all :class:`Actor` and :class:`Monitor`.

.. _design-arbiter:

To access the :class:`Arbiter`, from the main process, one can use the
:func:`arbiter` high level function::

    >>> arbiter = pulsar.arbiter()
    >>> arbiter.running()
    False
    

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
Each actor has its own :attr:`Actor.event_loop`, an instance of :class:`EventLoop`,
which can be used to register handlers on file descriptors.
The :attr:`Actor.event_loop` is initiated just after forking (or after the
actor's thread starts for thread-based actors).
Pulsar :class:`EventLoop` will be following pep-3156_ guidelines.

.. _iobound:

IO-bound
~~~~~~~~~~~~~~~
The most common usage for an :class:`Actor` is to handle Input/Output
events on file descriptors. An :attr:`Actor.event_loop` tells
the operating system (through `epoll` or `select`) that it should be notified
when a new connection is made, and then it goes to sleep.
Serving the new request should occur as fast as possible so that other
connections can be served simultaneously. 

.. _cpubound:

CPU-bound
~~~~~~~~~~~~~~~
Another way for an actor to function is to use its of :attr:`Actor.thread_pool`
to perform CPU intensive operations, such as calculations, data manipulation
or whatever you need them to do.
CPU-bound :class:`Actors` have the following properties:

* Their :attr:`Actor.event_loop` listen for requests on file descriptors
  as usual.
* The threads in the :attr:`Actor.thread_pool` use a specialised :class:`EventLoop`
  which listen for events on a message queue.


.. _design-mailbox:

Mailbox
~~~~~~~~~~~~~~
Each :class:`Actor`, with the only exception of :class:`Monitor`, have its own
:attr:`Actor.mailbox`, an asynchronous client of the :class:`Arbiter`
mailbox server. Check the :ref:`message passing documentation <tutorials-messages>`
for more information.


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

An :class:`Actor` exposes three :ref:`one time events <one-time-event>`
which can be used to customise its behaviour. These functions do nothing in the
standard :class:`Actor` implementation. 

**start**

Fired just before the actor starts its :ref:`event loop <eventloop>`.
This function can be used to setup
the application and register event handlers. For example, the
:ref:`socket server application <apps-socket>` creates the server and register
its file descriptor with the :attr:`Actor.event_loop`.

 
**stopping**

Fired when the :class:`Actor` starts stopping.

**stop**

Fired just before the :class:`Actor` is garbage collected
 


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
    
    send('monitor', 'run', dosomething, *args, **kwargs)
    

.. _actor_stop_command:

**stop**

Tell the remote actor ``abc`` to gracefully shutdown::

    send('abc', 'stop')
    
    
Asynchronous Components
===============================

Exceptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two categories of exceptions in Python: those that derive from the
:class:`Exception` class and those that derive from :class:`BaseException`.
Exceptions deriving from Exception will generally be caught and handled
appropriately; for example, they will be passed through by :class:`Deferred`,
and they will be logged and ignored when they occur in a callback.

However, exceptions deriving only from BaseException are never caught,
and will usually cause the program to terminate with a traceback.
(Examples of this category include KeyboardInterrupt and SystemExit;
it is usually unwise to treat these the same as most other exceptions.)


.. _design-application:

Application Framework
=============================

To aid the development of applications running on top of pulsar concurrent
framework, the library ships with the :class:`Application` class.



.. _pep-3156: http://www.python.org/dev/peps/pep-3156/