.. _odm:

=====================
Object Data Mapper
=====================

.. automodule:: pulsar.apps.data.odm

Features
===================
* Built on top of pulsar :ref:`data store client api <data-stores>`
* Presents a method of associating user-defined Python classes with data-stores
  **collections**/**tables**
* An instance of a :class:`.Model` is mapped into an **item**/**rows**
  in its corresponding collection/table.
* Allows the use different stores for different models
* Design to be fast and lightweight

Getting Started
=========================

The first step is to create a :class:`.Model` to play with::

    from pulsar.apps.data import odm

    class User(odm.Model):
        id = odm.CharField(primary_key=True)
        username = odm.CharField()
        password = odm.CharField()
        email = odm.CharField()


API
=========================

.. module:: pulsar.apps.data.odm.model

Model
~~~~~~~~~~~~~~~

.. autoclass:: Model
   :members:
   :member-order: bysource


.. module:: pulsar.apps.data.odm.mapper

Manager
~~~~~~~~~~~~~~~

.. autoclass:: Manager
   :members:
   :member-order: bysource


Mapper
~~~~~~~~~~~~~~~

.. autoclass:: Mapper
   :members:
   :member-order: bysource


.. module:: pulsar.apps.data.odm.transaction

Transaction
~~~~~~~~~~~~~~~

.. autoclass:: Transaction
   :members:
   :member-order: bysource
