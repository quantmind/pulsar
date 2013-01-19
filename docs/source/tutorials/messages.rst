.. module:: pulsar

.. _tutorials-messages:

=======================
Actor messages
=======================

Actors communicate with each other by sending and receiving messages.
Messages are sent by using the :func:`send` function.
Message sending is asynchronous and safe, the message is guaranteed to
eventually reach the recipient, provided that the recipient exists.

The implementation details are outlined below:

* The :class:`Arbiter` mailbox is a TCP server accepting connections from remote
  actors.
* The :attr:`Actor.mailbox` is a client of the arbiter mailbox server.
* When an actor sends a message to another actor, the arbiter mailbox behaves like
  a proxy server by routing the message to the targeted actor.
* The :attr:`Actor.mailbox` for :ref:`CPU bound actors <cpubound>` has its hown
  separate :class:`EventLoop` on a separate thread of execution. In other
  words, it does not share it with the actor so that I/O requests
  can be processed even if the actor is performing a long calculation.