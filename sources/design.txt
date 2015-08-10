.. _design:

.. module:: pulsar

=====================
Design
=====================

Pulsar implements two layers of components on top of python :mod:`asyncio`
module:

* :ref:`The actor layer <design-actor>` provides parallel execution in
  processes and threads and uses the :mod:`asyncio` and :mod:`multiprocessing`
  modules as building blocks.
* :ref:`The application framework <design-application>` is built on top of
  the actor layer and it is based on the higher level
  :class:`.Application` class which provides an elegant API for speeding
  up development of asynchronous and parallel software.

.. contents::
   :local:
   :depth: 2

.. _design-actor:

Actors
=================

An :class:`.Actor` is the atom of pulsar's concurrent computation,
they do not share state between them, communication is achieved via asynchronous
:ref:`inter-process message passing <tutorials-messages>`,
implemented using :mod:`asyncio` socket utilities.

Messages are exchanged using single bidirectional connections between any actor and are
encoded using the unmasked websocket protocol as the
:ref:`actor messages <tutorials-messages>` tutorial highlights.
A pulsar actor can be process based as well as thread based and
can perform one or many activities.

.. image:: _static/actors.svg
  :width: 600 px

The theory
------------------
The actor model is the cornerstone of the Erlang programming language.
Python has very few implementation and all of them seem quite limited in scope.

.. epigraph::

    The Actor model in computer science is a mathematical model of concurrent
    computation that treats "actors" as the universal primitives of concurrent
    digital computation: in response to a message that it receives, an actor
    can make local decisions, create more actors, send more messages, and
    determine how to respond to the next message received.

    -- Wikipedia

**Actor's properties**

* Each actor has its own ``process`` (not intended as an OS process) and they
  don't shares state between them.
* Actors can change their own states.
* Actors can create other actors and when they do that they receive back the new actor address.
* Actors exchange messages in an asynchronous fashion.

**Why would one want to use an actor-based system?**

* No shared memory and therefore locking is not required.
* Race conditions greatly reduced.
* It greatly simplifies the control flow of a program, each actor has its own
  process (flow of control).
* Easy to distribute, across cores, across program boundaries, across machines.
* It simplifies error handling code.
* It makes it easier to build fault-tolerant systems.

.. _concurrency:

Implementation
------------------
An actor can be **processed based** (default) or **thread based** and controls
one running :ref:`event loop <asyncio-event-loop>`.
To obtain the actor controlling the current thread::

    actor = pulsar.get_actor()

When a new processed-based actor is created, a new process is started and the
actor takes control of the main thread of that new process. On the other hand,
thread-based actors always exist in the master process (the same process
as the arbiter) and control threads other than the main thread.

An :class:`.Actor` can control more than one thread if it needs to, via the
:meth:`~.Actor.executor` as explained in the :ref:`CPU bound <cpubound>`
paragraph.

.. _actor-io-thread:

.. note::

    Regardless of the type of concurrency, an actor always controls at least
    one thread, the **actor io thread**. In the case of process-based actors
    this thread is the main thread of the actor process.

An actor is a :ref:`async object <async-object>` and therefore it has
a :attr:`~.Actor._loop`
attribute, which can be used to register handlers on file descriptors.
The :attr:`.Actor._loop` is created just after forking (or after the
actor's thread starts for thread-based actors).

.. _iobound:

IO-bound
------------------
The most common usage for an :class:`.Actor` is to handle Input/Output
events on file descriptors. An :attr:`.Actor._loop` tells
the operating system (through ``epoll`` or ``select``) that it should be notified
when a new connection is made, and then it goes to sleep.
Serving the new request should occur as fast as possible so that other
connections can be served simultaneously.

.. _cpubound:

CPU-bound
------------------
Another way for an actor to function is to use its :meth:`~.Actor.executor`
to perform CPU intensive operations, such as calculations, data manipulation
or whatever you need them to do.


.. _actor-periodic-task:

Periodic task
------------------

Each :class:`.Actor`, including the :class:`.Arbiter` and :class:`.Monitor`,
perform one crucial periodic task at given intervals. The next
call of the task is stored in the :attr:`.Actor.next_periodic_task`
attribute.

Periodic task are implemented by the :meth:`Concurrency.periodic_task` method.

.. _design-arbiter:

The Arbiter
------------------
When using pulsar actor layer, you need to use pulsar in **server state**,
that is to say, there will be a centralised **Arbiter** controlling the main
:ref:`event loop <asyncio-event-loop>` in the **main thread** of the
**master process**.
The arbiter is a specialised :class:`.Actor`
which control the life of all :class:`.Actor` and
:ref:`monitors <design-monitor>`

To access the arbiter, from the main process, one can use the
:func:`.arbiter` high level function::

    >>> arbiter = pulsar.arbiter()
    >>> arbiter.is_running()
    False


.. _design-application:

Application Framework
=============================

To aid the development of applications running on top of pulsar concurrent
framework, the library ships with the :class:`.Application` class.
Applications can be of any sorts or forms and the library is shipped
with several battery included examples in the pulsar.apps module.

When an Application is called for the first time, a new :ref:`monitor <design-monitor>`
is added to the :ref:`arbiter <design-arbiter>`, ready to perform its duties.

.. _design-monitor:

Monitors
------------------

Monitors are specialised actors which share the :ref:`arbiter <design-arbiter>`
event loop and therefore live in the main thread of the master process
of your application.

It is possible to configure pulsar so that the arbiter delegates the
management of some actors to monitors.
The :ref:`application layer <design-application>` is designed specifically
to obtain such delegation in a straightforward way with an efficient
and elegant API.

.. image:: _static/monitors.svg
  :width: 600 px


Internals
=============

.. _design-spawning:

Spawning
-------------

Spawning a new actor is achieved via the :func:`.spawn` function::

    from pulsar import spawn

    def task(actor, exc=None):
        # do something useful here
        ...

    ap = spawn(periodic_task=task)

The value returned by :func:`.spawn` is an :class:`~asyncio.Future`,
called back once the remote actor has started.
The callback will be an :class:`.ActorProxy`, a lightweight proxy
for the remote actor.

When spawning from an actor other than the :ref:`arbiter <design-arbiter>`,
the workflow of the :func:`.spawn` function is as follow:

* :func:`.send` a message to the :ref:`arbiter <design-arbiter>` to spawn
  a new actor.
* The arbiter spawn the actor and wait for the actor's
  :ref:`handshake <handshake>`. Once the hand shake is done, it sends the
  response (the :class:`.ActorProxy` of the
  spawned actor) to the original actor.

.. _handshake:

Handshake
--------------

The actor **hand-shake** is the mechanism with which an :class:`.Actor`
register its :ref:`mailbox address <tutorials-messages>` with its manager.
The actor manager is either a :class:`.Monitor` or the
:ref:`arbiter <design-arbiter>` depending on which spawned the actor.

The handshake occurs when the monitor receives, for the first time,
the actor :ref:`notify message <actor_notify_command>`.

For the curious, the handshake is responsible for setting the
:attr:`.ActorProxyMonitor.mailbox` attribute.

If the hand-shake fails, the spawned actor will eventually stop.


.. _actor-hooks:

Hooks
----------

An :class:`.Actor` exposes three :ref:`one time events <one-time-event>`
which can be used to customise its behaviour and two
:ref:`many times event <many-times-event>` used when accessing actor
information and when the actor spawn other actors.
Hooks are passed as key-valued parameters to the :func:`.spawn` function.

**start**

Fired just after the actor has received the
:ref:`hand-shake from its monitor <handshake>`. This hook can be used to setup
the application and register event handlers. For example, the
:ref:`socket server application <apps-socket>` creates the server and register
its file descriptor with the :attr:`.Actor._loop`.

This snippet spawns a new actor which starts an
:ref:`Echo server <tutorials-writing-clients>`::

    from functools import partial

    from pulsar import spawn, TcpServer

    def create_echo_server(address, actor, _):
        '''Starts an echo server on a newly spawn actor'''
        server = TcpServer(actor.event_loop, address[0], address[1],
                           EchoServerProtocol)
        yield server.start_serving()
        actor.servers['echo'] = server
        actor.extra['echo-address'] = server.address

    proxy = spawn(start=partial(create_echo_server, 'localhost:9898'))

The :class:`.EchoServerProtocol` is introduced in the
:ref:`echo server and client tutorial <tutorials-writing-clients>`.

**stopping**

Fired when the :class:`.Actor` starts stopping.

**periodic_task**

Fired at every actor periodic task (More docs here)

**on_info**

Fired every time the actor status information is accessed via the
:ref:`info command <actor_info_command>`::

    def extra_info(actor, info=None):
        info['message'] = 'Hello'

    proxy = spawn(on_info=extra_info)

The hook must accept the actor as first parameter and the ``key-valued``
parameter ``info`` (a dictionary).

**on_params**

Fired every time an actor is about to spawn another actor. It can be used to
add additional key-valued parameters passed to the :func:`.spawn`
function.

.. _actor_commands:

Commands
---------------

An :class:`.Actor` communicates with another remote :class:`.Actor` by *sending*
an **action** to perform. This action takes the form of a **command** name and
optional positional and key-valued parameters. It is possible to add new
commands via the :class:`.command` decorator as explained in the
:ref:`api documentation <api-remote_commands>`.


ping
~~~~~~~~~

Ping the remote actor ``abcd`` and receive an asynchronous ``pong``::

    send('abcd', 'ping')


echo
~~~~~~~~~~~

received an asynchronous echo from a remote actor ``abcd``::

    send('abcd', 'echo', 'Hello!')


.. _actor_info_command:

info
~~~~~~~~~~~~~

Request information about a remote actor ``abcd``::

    send('abcd', 'info')

The asynchronous result will be called back with the dictionary returned
by the :meth:`.Actor.info` method.

.. _actor_notify_command:

notify
~~~~~~~~~~~~~~~~

This message is used periodically by actors, to notify their manager. If an
actor fails to notify itself on a regular basis, its manager will shut it down.
The first ``notify`` message is sent to the manager as soon as the actor is up
and running so that the :ref:`handshake <handshake>` can occur.


.. _actor_run_command:

run
~~~~~~~~~~

Run a function on a remote actor. The function must accept actor as its
initial parameter::

    def dosomething(actor, *args, **kwargs):
        ...

    send('monitor', 'run', dosomething, *args, **kwargs)


.. _actor_stop_command:

stop
~~~~~~~~~~~~~~~~~~

Tell the remote actor ``abc`` to gracefully shutdown::

    send('abc', 'stop')

.. _exception-design:

Exceptions
------------------

There are two categories of exceptions in Python: those that derive from the
:class:`Exception` class and those that derive from :class:`BaseException`.
Exceptions deriving from Exception will generally be caught and handled
appropriately; for example, they will be passed through by a
:class:`~asyncio.Future`,
and they will be logged and ignored when they occur in a callback.

However, exceptions deriving only from BaseException are never caught,
and will usually cause the program to terminate with a traceback.
(Examples of this category include KeyboardInterrupt and SystemExit;
it is usually unwise to treat these the same as most other exceptions.)

.. _async-object:

Async Objects
------------------
An asynchronous object is any instance which exposes
the :attr:`~.AsyncObject._loop` attribute.
This attribute is the :ref:`event loop <asyncio-event-loop>` where
the instance performs its asynchronous operations, whatever they may be.

For example this is a class for valid async objects::

    from pulsar import get_event_loop, new_event_loop


    class SimpleAsyncObject:

        def __init__(self, loop=None):
            self._loop = loop or get_event_loop() or new_event_loop()

Properties:

* Several classes in pulsar are async objects, for example: :class:`.Actor`,
  :class:`.Connection`, :class:`.ProtocolConsumer`, :class:`.Store`
  and so forth
* A :class:`~asyncio.Future` is an async object
* However an async object **is not** necessarily a :class:`~asyncio.Future`
* When they use and :func:`.task` decorators for their methods,
  :attr:`~.AsyncObject._loop` attribute is used to run the method
* Pulsar provides the :class:`.AsyncObject` signature class,
  however it is not a requirement to derive from it

.. note::

    An async object can also run its asynchronous methods in a synchronous
    fashion. To do that, one should pass a bright new event loop during
    initialisation. Check :ref:`synchronous components <tutorials-synchronous>`
    for further details.



.. _pep-3156: http://www.python.org/dev/peps/pep-3156/
