.. _design:

.. module:: pulsar

=====================
Design
=====================

Pulsar implements three layers of components for building a vast array
of parallel and asynchronous applications. Each layer depends on the
previous ones but it is independent on the layers above it. The three layers
are:

* :ref:`The asynchronous layer <async-layer>` forms the building blocks
  of asynchronous execution. The main classes here are the :class:`Deferred`
  and the :class:`EventLoop`.
* :ref:`The actor layer <design-actor>` provides parallel execution in
  processes and threads and uses the :ref:`the asynchronous layer <async-layer>`
  as building block.
* The last layer, built on top of the first two, is based on the higher level
  :class:`apps.Application` class.
   
.. _async-layer:

Asynchronous Components
===============================

Pulsar can be used as a stand-alone asynchronous library, without using
:ref:`actors <design-actor>` to provide parallel execution and more information
can be found in the :ref:`asynchronous components <tutorials-coroutine>`
tutorial. 

.. _eventloop:

Event Loop
~~~~~~~~~~~~~~~
The pulsating heart of the asynchronous framework.
Pulsar :class:`EventLoop` will be following pep-3156_ guidelines.

Deferred
~~~~~~~~~~~~
Designed along the lines of `twisted deferred`_, this class is a callback which
will be put off until later. Pulsar has three types of deferred:

* A vanilla :class:`Deferred`, similar to twisted deferred.
* A :class:`Task`, a specialised deferred which consume a :ref:`coroutine <coroutine>`.
* A :class:`MultiDeferred` for managing collections of independent asynchronous
  components.

.. _twisted deferred: http://twistedmatrix.com/documents/current/core/howto/defer.html
    
.. _design-actor:

Actors
=================

An :class:`Actor` is the atom of pulsar's concurrent computation,
they do not share state between them, communication is achieved via asynchronous
:ref:`inter-process message passing <tutorials-messages>`,
implemented using the standard python socket library. A pulsar actor can be
process based as well as thread based and can perform one or many activities.

The Theory
~~~~~~~~~~~~~~~~~
The actor model is the cornerstone of the Erlang programming language.
Python has very few implementation and all of them seems quite limited in scope.

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
* It greatly simplify the control flow of a program, each actor has its own process (flow of control).
* Easy to distribute, across cores, across program boundaries, across machines.
* It simplifies error handling code.
* It makes it easier to build fault-tolerant systems.

.. _arbiter:

The Arbiter
~~~~~~~~~~~~~~~~~
When using pulsar actor layer, you need to use pulsar in **server state**,
that is to say, there will be a centralised :class:`Arbiter` controlling the main
:class:`EventLoop` in the **main thread** of the **master process**.
The arbiter is a specialised :class:`Actor`
which control the life all :class:`Actor` and :class:`Monitor`.

.. _design-arbiter:

To access the :class:`Arbiter`, from the main process, one can use the
:func:`arbiter` high level function::

    >>> arbiter = pulsar.arbiter()
    >>> arbiter.running()
    False
    
.. _concurrency:

Implementation
~~~~~~~~~~~~~~~~~~
An actor can be **processed based** (default) or **thread based** and control
at least one running :class:`EventLoop`.
To obtain the actor controlling the current thread::

    actor = pulsar.get_actor()
    
When a new processed-based actor is created, a new process is started and the
actor takes control of the main thread of that new process. On the other hand,
thread-based actors always exist in the master process (the same process
as the arbiter) and control threads other than the main thread.

An actor can control more than one thread if it needs to, via the
:attr:`Actor.thread_pool` as explained in the :ref:`CPU bound <cpubound>`
paragraph.
The actor :ref:`event loop <eventloop>` is installed in all threads controlled
by the actor so that when the ``get_event_loop`` function is invoked on
these threads it returns the event loop of the controlling actor.

.. _actor-io-thread:

.. note::

    Regardless of the type of concurrency, an actor always controls at least
    one thread, the **actor io thread**. In the case of process-based actors
    this thread is the main thread of the actor process.
    
Each actor has its own :attr:`Actor.event_loop`, an instance of :class:`EventLoop`,
which can be used to register handlers on file descriptors.
The :attr:`Actor.event_loop` is created just after forking (or after the
actor's thread starts for thread-based actors).

.. _iobound:

IO-bound
~~~~~~~~~~~~~~~
The most common usage for an :class:`Actor` is to handle Input/Output
events on file descriptors. An :attr:`Actor.event_loop` tells
the operating system (through ``epoll`` or ``select``) that it should be notified
when a new connection is made, and then it goes to sleep.
Serving the new request should occur as fast as possible so that other
connections can be served simultaneously. 

.. _cpubound:

CPU-bound
~~~~~~~~~~~~~~~
Another way for an actor to function is to use its :attr:`Actor.thread_pool`
to perform CPU intensive operations, such as calculations, data manipulation
or whatever you need them to do.
CPU-bound :class:`Actor` have the following properties:

.. _request-loop:

* Their :attr:`Actor.event_loop` listen for requests on file descriptors
  as usual and it is running (and installed) in the :ref:`actor io thread <actor-io-thread>`
  as usual.
* The threads in the :attr:`Actor.thread_pool` install an additional :class:`EventLoop`
  which listen for events on a message queue.
  Pulsar refers to this specialised event loop as the **request loop**.
  
.. note::

    A CPU-bound actor controls more than one thread, the :ref:`IO thread <actor-io-thread>`
    which runs the actor main event loop for listening to events on file descriptors and
    one or more threads for performing CPU-intensive calculations. These CPU-threads
    have installed two events loops: the event loop running on the
    :ref:`IO thread <actor-io-thread>` and the :ref:`request-loop <request-loop>`.

The :attr:`Actor.thread_pool` needs to be initialised via the
:attr:`Actor.create_thread_pool` method before it can be used.


.. _actor-periodic-task:

Periodic task
~~~~~~~~~~~~~~~~~~~~~~

Each :class:`Actor`, including the :class:`Arbiter` and :class:`Monitor`,
perform one crucial periodic task at given intervals. The next
call of the task is stored in the :class:`Actor.next_periodic_task`
attribute.

Periodic task are implemented by the :class:`Concurrency.periodic_task` method.

.. _design-spawning:

Spawning
==============

Spawning a new actor is achieved via the :func:`spawn` function::
    
    from pulsar import spawn
    
    class PeriodicTask:
    
        def __call__(self, actor):
            actor.event_loop.call_repeatedly(2, self.task)
            
        def task(self):
            # do something useful here
            ...
        
    ap = spawn(start=PeriodicTask())
    
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

.. _handshake:

Handshake
~~~~~~~~~~~~~~~

The actor **hand-shake** is the mechanism with which an :class:`Actor` register
its :ref:`mailbox address <tutorials-messages>` with its manager.
The actor manager is either a :class:`Monitor` or the :class:`Arbiter`
depending on which spawned the actor.

The handshake occurs when the monitor receives, for the first time,
the actor :ref:`notify message <actor_notify_command>`.

For the curious, the handshake is responsible for setting the
:class:`ActorProxyMonitor.mailbox` attribute.

If the hand-shake fails, the spawned actor will eventually stop.


.. _actor-hooks:

Hooks
~~~~~~~~~~~~~~~~~~~

An :class:`Actor` exposes three :ref:`one time events <one-time-event>`
which can be used to customise its behaviour. Hooks are passed as key-valued
parameters to the :func:`spawn` function.

**start**

Fired just after the actor has received the
:ref:`hand-shake from its monitor <handshake>`. This hook can be used to setup
the application and register event handlers. For example, the
:ref:`socket server application <apps-socket>` creates the server and register
its file descriptor with the :attr:`Actor.event_loop`.

This snippet spawns a new actor which starts an
:ref:`Echo server <tutorials-writing-clients>`::

    from functools import partial
    
    from pulsar import spawn, TcpServer
    
    def create_echo_server(address, actor):
        '''Starts an echo server on a newly spawn actor'''
        server = TcpServer(actor.event_loop, address[0], address[1],
                           EchoServerProtocol)
        yield server.start_serving()
        actor.servers['echo'] = server
        actor.extra['echo-address'] = server.address
        
    proxy = spawn(start=partial(create_echo_server, 'localhost:9898'))
    
The :class:`examples.echo.manage.EchoServerProtocol` is introduced in the
:ref:`echo server and client tutorial <tutorials-writing-clients>`.


.. note::

    Hooks are function receiving as only argument the actor which invokes them.
    
**stopping**

Fired when the :class:`Actor` starts stopping.

**stop**

Fired just before the :class:`Actor` is garbage collected
 


.. _actor_commands:

Commands
===============

An :class:`Actor` communicates with another remote :class:`Actor` by *sending*
an **action** to perform. This action takes the form of a **command** name and
optional positional and key-valued parameters. It is possible to add new
commands via the :class:`pulsar.command` decorator as explained in the
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
by the :meth:`Actor.info` method.
    
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

Run a function on a remote actor. The function must accept actor as its initial parameter::

    def dosomething(actor, *args, **kwargs):
        ...
    
    send('monitor', 'run', dosomething, *args, **kwargs)
    

.. _actor_stop_command:

stop
~~~~~~~~~~~~~~~~~~

Tell the remote actor ``abc`` to gracefully shutdown::

    send('abc', 'stop')
    
.. _monitor:

Monitors
==============

    
.. _exception-design:

Exceptions
=====================

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
framework, the library ships with the :class:`pulsar.apps.Application` class.



.. _pep-3156: http://www.python.org/dev/peps/pep-3156/