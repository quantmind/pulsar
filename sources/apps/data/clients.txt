.. _data-stores:

=====================
Datastore Clients
=====================

.. automodule:: pulsar.apps.data.store

Getting Started
=====================

.. _connection-string:

Connection String
~~~~~~~~~~~~~~~~~~~~~~~

The connection string is a way to specify the various parameters for the
data store connection. All :class:`.Store` support a connection string
of the form::

    <scheme>://<username>:<password>@<host>/<database>?param1=...&param2=...


backend to use. Redis supports the following
parameters:

* ``db``, the database number.
* ``namespace``, the namespace for all the keys used by the backend.
* ``password``, database password.
* ``timeout``, connection timeout (0 is an asynchronous connection).

A full connection string could be::

    redis://127.0.0.1:6379?db=3&password=bla&namespace=test.&timeout=5


Implement a Store
==================

When implementing a new :class:`.Store` there are several methods which need
to be covered:

* :meth:`~Store.ping` to check if the server is available
* :meth:`~Store.connect` to create a new connection
* :meth:`~Store.execute` to execute a command on the store server

These additional methods must be implemented only if the store supports
the :ref:`object data mapper <odm>`:

* :meth:`~Store.execute_transaction` execute a transaction

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

.. autofunction:: pulsar.apps.data.stores.pulsards.startds.start_store


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

