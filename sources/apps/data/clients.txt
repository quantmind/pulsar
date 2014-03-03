.. _data-stores:

=====================
Datastore Clients
=====================

.. automodule:: pulsar.apps.data.stores.pulsards.base

Implement a Store
==================

When implementing a new :class:`.Store` there are several methods which need
to be covered:

* :meth:`Store.connect` to create a new connection
* :meth:`Store.execute` to execute a command on the store server

A new store needs to be registered via the :func:`register_store`
function.

All registered data stores are stored in the ``data_stores`` dictionary::

    from pulsar.apps.data import data_stores

Pulsar provides two implementations, the redis client and the pulsards client.


.. _apps-pubsub:

Publish/Subscribe
=====================

A :class:`.Store` can implement the :meth:`~Store.pubsub` method to return
a valid :class:`.PubSub` handler.


API
============

Create store
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: create_store


Start store
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: start_store


Register a new store
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: register_store


Store
~~~~~~~~~~~~~~~

.. autoclass:: Store
   :members:
   :member-order: bysource


Command
~~~~~~~~~~~~~~~

.. autoclass:: Command
   :members:
   :member-order: bysource


PubSub
~~~~~~~~~~~~~~~

.. autoclass:: PubSub
   :members:
   :member-order: bysource

