.. _apps-pubsub:

Publish/Subscribe
=====================

A data :class:`.Store` can implement the :meth:`~.RemoteStore.pubsub` method to return
a valid :class:`.PubSub` handler.


Channels
-----------

Channels are an high level object which uses a :class:`.PubSub` handler to manage
events on channels. They are useful for:

* Reducing the number of channels to subscribe to by introducing channel events
* Manage registration and un-registration to channel events
* Handle reconnection with exponential back-off

API
----

PubSub
~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.PubSub
   :members:
   :member-order: bysource


Channels
~~~~~~~~~~~~~~~

.. autoclass:: pulsar.apps.data.Channels
   :members:
   :member-order: bysource
