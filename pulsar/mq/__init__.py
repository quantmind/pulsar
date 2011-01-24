"""An Message Queue micro-library based on AMQP

The Advanced Message Queuing Protocol (AMQP) is an open Internet Protocol for Business
Messaging.

http://www.amqp.org.


Network
================

The AMQP Network consists ``Containers`` which holds ``Nodes`` connected via ``Links``.

 * Nodes are named entities responsible for the safe storage and/or delivery
   of Messages.
 * A Link is a unidirectional route between two Nodes along which Messages may travel
   if they meet the entry criteria of the Link.
 * Nodes exist within a Container, and each Container may hold many Nodes.
 * Examples of Nodes are ``Producers``, ``Consumers``, and ``Queues``.
 
Client Application
=====================
 * A client application is an example of a Container.
 * Producers and Consumers are nodes within a client Application that generate and
   process Messages.
   
Broker
===========
 * Queues are entities within a Broker that store and forward Messages.
  
Transport
======================
   
The Transport Specification defines a peer-to-peer protocol for transferring Messages
between Nodes in the AMQP network.

Type conversion

http://www.amqp.org/spec/1-0-draft/type-mappings/mapped-languages.xml
"""
from .abstract import AMQPobject, Node, Connection
