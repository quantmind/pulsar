"""\
A message-queue interface library.
Designed to comply as much as possible to AMQP_,
the Advanced Message Queuing Protocol, an open Internet
Protocol for Business Messaging.

.. _AMQP: http://www.amqp.org.


Overview
=============

AMQP defines several **layers** and **entities** which can get quite confusing at first.
The first concept to get familiar with are the entities of a AMQP network.

Network
~~~~~~~~~~~~~

Entities in a :mod:`pulsar.mq` network are: **containers**, **nodes** and **links**.
This is in line to what entities are in AMQP. Here the important facts:

* `Nodes` exist within a `containers`, and each container may hold many nodes.
  There are two container of interests: :class:`pulsar.mq.Broker`
  responsable sending messages and :class:`pulsar.mq.Client`
  responsable for producing and consuming messages. 
* :class:`pulsar.mq.Node` is a named entity responsible for the safe storage
  and/or delivery of Messages. Messages can originate from, terminate at,
  or be relayed by Nodes.
* A :class:`pulsar.mq.Link` is a unidirectional route between two Nodes along
  which Messages may travel if they meet the entry criteria of the link.
* Examples of nodes in the :class:`pulsar.mq.Client` container are
  :class:`pulsar.mq.Producer` and :class:`pulsar.mq.Consumer`,
  while in the :class:`pulsar.Broker` container :class:`pulsar.Queue`.

the first layer defines **Brokers** and **Client Applications**.
Broker are responsible for
 
Client Application
~~~~~~~~~~~~~

 * A client application is an example of a Container.
 * Producers and Consumers are nodes within a client Application that generate and
   process Messages.
   
Broker
~~~~~~~~~~~~~

 * Queues are entities within a Broker that store and forward Messages.
  
Transport
~~~~~~~~~~~~~
   
The Transport Specification defines a peer-to-peer protocol for transferring Messages
between Nodes in the AMQP network.

Type conversion

http://www.amqp.org/spec/1-0-draft/type-mappings/mapped-languages.xml
"""
from .entity import *
from .message import *
from .channel import *
#from .abstract import AMQPobject, Node, Connection

