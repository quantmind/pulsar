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
* An instance of a :class:`.Model` is mapped into an **item**/**row**
  in its corresponding collection/table.
* Allows the use of different stores for different models
* Design to be fast, lightweight and non-intrusive

Getting Started
=========================

The first step is to create a :class:`.Model` to play with::

    from pulsar.apps.data import odm

    class User(odm.Model):
        username = odm.CharField(index=True)
        password = odm.CharField()
        email = odm.CharField(index=True)
        is_active = odm.BoolField(default=True)


Mapper & Managers
=======================

A :class:`.Model` such as the ``User`` class defined above has no information
regarding database, it is purely a dictionary with additional information
about fields.


.. _odm-registration:

Registration
~~~~~~~~~~~~~~~~~~~

Registration consists in associating a :class:`.Model` to a :class:`.Manager`
via a :class:`.Mapper`. In this way one can have a group of models associated
with their managers pointing at their, possibly different, back-end servers.
Registration is straightforward and it is achieved by::

    from pulsar.apps.data import odm

    models = odm.Mapper('redis://127.0.0.1:6379/7')

    models.register(User)
    models.register(Group)

The :ref:`connection string <connection-string>` passed as first argument when
initialising a :class:`.Mapper`, is the default data store of that
:class:`.Mapper`.
It is possible to register models to a different data-staores by passing
a connection string to the :meth:`.Mapper.register` method::

    models.register(MyModel, 'redis://127.0.0.1:6379/8')


Accessing managers
~~~~~~~~~~~~~~~~~~~~~~~~
Given a ``models`` :class:`.Mapper` there are two ways one can access a
model :class:`.Manager` to perform database queries.

* **Dictionary interface** is the most straightforward and intuitive way::


    # Create a Query for Instrument
    query = models[Instrument].query()
    #
    # Create a new Instrument and save it to the backend server
    inst = models[Instrument].new(...)


* **Dotted notation** is an alternative and more pythonic way of achieving the
  same manager via an attribute of the :class:`.Mapper`, the attribute
  name is given by the :class:`.Model` metaclass :class:`~Meta.name`.
  It is, by default, the class name of the model in lower case::

      query = models.instrument.query()
      inst = models.instrument.new(...)

This interface is less verbose than the :ref:`dictionary notation <router-dict>`
and, importantly, it reduces to zero the imports one has to write on python
modules using your application, in other words it makes your application
less dependent on the actual implementation of :class:`StdModel`.

Create an instance
~~~~~~~~~~~~~~~~~~~~~~

When creating a new instance of model the callable method its registered
:class:`.Manager` should be used::

    pippo = models.user(username='pippo', email='pippo@bla.com')

``pippo`` is a instance not yet persistent in the data store.


Multiple backends
~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.Mapper` allows to use your models in several different back-ends
without changing the way you query your data. In addition it allows to
specify different back-ends for ``write`` operations and for ``read`` only
operations.

To specify a different back-end for read operations one registers a model in
the following way::

    models.register(Position, 'redis://127.0.0.1:6379?db=8&password=bla',
                    'redis://127.0.0.1:6380?db=1')


.. _custom-manager:

Custom managers
~~~~~~~~~~~~~~~~~~~~~~~

When a :class:`.Mapper` registers a :class:`.Model`, it creates a new
instance of a :class:`.Manager` and add it to the dictionary of managers.
It is possible to supply a custom manager class by specifying the
``manager_class`` attribute on the :class:`.Model`::

    from pulsar.apps.data import odm

    class CustomManager(odm.Manager):

        def special_query(self, ...):
            return self.query().filter(...)


    class MyModel(odm.Model):
        ...

        manager_class = CustomManager


Quering Data
==================



API
=========================

.. module:: pulsar.apps.data.odm.model

Model
~~~~~~~~~~~~~~~

.. autoclass:: Model
   :members:
   :member-order: bysource


Model Meta
~~~~~~~~~~~~~~~

.. autoclass:: ModelMeta
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


.. module:: pulsar.apps.data.odm.fields

Field
~~~~~~~~~~~~~~~

.. autoclass:: Field
   :members:
   :member-order: bysource


CharField
~~~~~~~~~~~~~~~

.. autoclass:: CharField
   :members:
   :member-order: bysource


IntegerField
~~~~~~~~~~~~~~~

.. autoclass:: IntegerField
   :members:
   :member-order: bysource


FloatField
~~~~~~~~~~~~~~~

.. autoclass:: FloatField
   :members:
   :member-order: bysource


ForeignKey
~~~~~~~~~~~~~~~

.. autoclass:: ForeignKey
   :members:
   :member-order: bysource


FieldError
~~~~~~~~~~~~~~~

.. autoclass:: FieldError
   :members:
   :member-order: bysource


.. module:: pulsar.apps.data.odm.query

Query
~~~~~~~~~~~

.. autoclass:: Query
   :members:
   :member-order: bysource


Compiled Query
~~~~~~~~~~~~~~~~~~

.. autoclass:: CompiledQuery
   :members:
   :member-order: bysource
