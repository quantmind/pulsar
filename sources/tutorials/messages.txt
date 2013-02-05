.. module:: pulsar

.. _tutorials-messages:

=======================
Actor messages
=======================

.. automodule:: pulsar.async.mailbox

.. module:: pulsar

Message sending is asynchronous and safe, the message is guaranteed to
eventually reach the recipient, provided that the recipient exists.

The implementation details are outlined below:

* Messages are sent via the :func:`send` function, which is a proxy for
  the :class:`Actor.send` method. Here is how you ping actor ``abc``::
      
      send('abc', 'ping')
        
* The :class:`Arbiter` mailbox is a TCP :class:`Server` accepting :class:`Connection`
  from remote actors.
* The :attr:`Actor.mailbox` is a client of the arbiter mailbox server.
* When an actor sends a message to another actor, the arbiter mailbox behaves like
  a proxy server by routing the message to the targeted actor.
* Communication is bidirectional and there is only one connection between
  the arbiter and any given actor.
* Messages are encoded and decoded using the unmasked websocket protocol implemented
  in :class:`pulsar.utils.websocket.FrameParser`.
* The :attr:`Actor.mailbox` for :ref:`CPU bound actors <cpubound>` has its hown
  separate :class:`EventLoop` on a separate thread of execution. In other
  words, it does not share it with the actor so that I/O requests
  can be processed even if the actor is performing a long calculation.