.. _apps-data-clients:

=====================
Datastore Clients
=====================

.. automodule:: pulsar.apps.data.stores.pulsarstore.base

Implement a Store
==================

When implementing a new :class:`.Store` there are several methods which need
to be covered:

 * :meth:`Store.connect` to create a new connection
 * :meth:`Store.execute` to execute a command on the store server

API
============

create store
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: create_store


start store
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: start_store


Store
~~~~~~~~~~~~~~~

.. autoclass:: Store
   :members:
   :member-order: bysource
