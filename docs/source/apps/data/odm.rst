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
  in its corresponding collection/table
* It allows the use of :ref:`different stores for different models <odm-multiple-backends>`
* Straightforward CRUD operations
* Design to be fast, lightweight and non-intrusive

.. _odm-intro:

Getting Started
=========================

The first step is to create the two :class:`.Model` we are going to use
throughout this tutorial::

    from datetime import datetime, timedelta

    from pulsar.apps.data import odm

    class User(odm.Model):
        username = odm.CharField(index=True)
        password = odm.CharField()
        email = odm.CharField(index=True, required=False)
        is_active = odm.BoolField(default=True)


    class Session(odm.Model):
        expiry = odm.DateTimeField(
            default=lambda: datetime.now() + timedelta(days=7))
        user = odm.ForeignKey(User)
        data = odm.JSONField()


The first model contains some basic information for a user, while the second
represents a session (think of it as a web session for example).


Mapper & Managers
=======================

A :class:`.Model`, such as the ``User`` class defined above, has no information
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

    models.register(User, Session)

The :ref:`connection string <connection-string>` passed as first argument when
initialising a :class:`.Mapper`, is the default :ref:`data store <data-stores>`
of that :class:`.Mapper`.
It is possible to register models to a different data-stores by passing
a ``store`` parameter to the :meth:`.Mapper.register` method::

    models.register(User, store='redis://127.0.0.1:6379/8')


Accessing managers
~~~~~~~~~~~~~~~~~~~~~~~~
Given a ``models`` :class:`.Mapper` there are two ways one can access a
model :class:`.Manager` to perform database queries.

.. _mapper-dict:

* **Dictionary interface** is the most straightforward and intuitive way::


    # Create a Query for Instrument
    query = models[User].query()
    #
    # Create a new Instrument and save it to the backend server
    inst = models[User].create(...)

.. _mapper-dotted:

* **Dotted notation** is an alternative and more pythonic way of achieving the
  same manager via an attribute of the :class:`.Mapper`, the attribute
  name is given by the :class:`.Model` metaclass :attr:`~.ModelMeta.name`.
  It is, by default, the class name of the model in lower case::

      query = models.user.query()
      user = models.user.create(...)

The :ref:`dotted notation <mapper-dotted>` is less verbose than the
:ref:`dictionary notation <mapper-dict>` and, importantly, it allows to
access models and their managers without the need to directly
import the model definition. All you need is the :class:`.Mapper`.
In other words it makes your application
less dependent on the actual implementation of a :class:`.Model`.

Create an instance
~~~~~~~~~~~~~~~~~~~~~~

When creating a new instance of model the callable method its registered
:class:`.Manager` should be used::

    pippo = models.user(username='pippo', email='pippo@bla.com')

``pippo`` is a instance not yet persistent in the data store.


.. _odm-multiple-backends:

Multiple backends
~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.Mapper` allows to use your models in several different back-ends
without changing the way you query your data. In addition it allows to
specify different back-ends for ``write`` operations and for ``read`` only
operations.

To specify a different back-end for read operations one registers a model in
the following way::

    models.register(User,
                    store='redis://127.0.0.1:6379/8,
                    read_store='redis://127.0.0.1:6380/1')


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


Querying Data
==================

.. _odm-search:

Full text search
~~~~~~~~~~~~~~~~~~~~


Relationships
==================

There are two :class:`.Field` which represent relationships between
:class:`.Model`.


.. _one-to-many:

One-to-many
~~~~~~~~~~~~~~

The :ref:`Session model <odm-intro>` in our example,
contains one :class:`.ForeignKey` field which represents a relationship
between the ``Session`` model and
the ``User`` model.

In the context of relational databases a
`foreign key <http://en.wikipedia.org/wiki/Foreign_key>`_ is
a referential constraint between two tables.
The same definition applies to pulsar odm. The field store the ``id`` of a
related :class:`.Model` instance.

**Key Properties**

* Behind the scenes, stdnet appends ``_id`` to the field name to create its
  field name in the back-end data-server. In other words, the
  :ref:`Session model <odm-intro>` is mapped into a data-store
  object with the following entries::

        {'id': ...,
         'user_id': ...,
         'expiry': ...
         'data': ...}

* The attribute of a :class:`ForeignKey` can be used to access the related
  object. Using the :ref:`router we created during registration <tutorial-registration>`
  we get a position instance::

        p = router.position.get(id=1)
        p.instrument    # an instance of Instrument

  The second statement is equivalent to::

        router.instrument.query().get(id=p.instrument_id)

  .. note::

    The loading of the related object is done, **once only**, the first time
    the attribute is accessed. This means, the first time you access a related
    field on a model instance, there will be a roundtrip to the backend server.

  Behind the scenes, this functionality is implemented by Python
  descriptors_. This shouldn't really matter to you, but we point it out here
  for the curious.

* Depending on your application, sometimes it makes a lot of sense to use the
  :ref:`load_related query method <performance-loadrelated>` to boost
  performance when accessing many related fields.

* When the object referenced by a :class:`ForeignKey` is deleted, stdnet also
  deletes the object containing the :class:`ForeignKey` unless the
  :class:`Field.required` attribute of the :class:`ForeignKey` field is set
  to ``False``.


.. _many-to-many:

Many-to-many
~~~~~~~~~~~~~~~~~~

The :class:`ManyToManyField` can be used to create relationships between
multiple elements of two models. It requires a positional argument, the class
to which the model is related.

Behind the scenes, stdnet creates an intermediary model to represent
the many-to-many relationship. We refer to this as the ``through model``.

Let's consider the following example::

    class Group(odm.StdModel):
        name = odm.SymbolField(unique=True)

    class User(odm.StdModel):
        name = odm.SymbolField(unique=True)
        groups = odm.ManyToManyField(Group, related_name='users')

Both the ``User`` class and instances of if have the ``groups`` attribute which
is an instance of A many-to-may :class:`stdnet.odm.related.One2ManyRelatedManager`.
Accessing the manager via the model class or an instance has different outcomes.


.. _through-model:

The through model
~~~~~~~~~~~~~~~~~~~~~~~

Custom through model
~~~~~~~~~~~~~~~~~~~~~~

In most cases, the standard through model implemented by stdnet is
all you need. However, sometimes you may need to associate data with the
relationship between two models.

For these situations, stdnet allows you to specify the model that will be used
to govern the many-to-many relationship and pass it to the
:class:`ManyToManyField` constructor via the ``through`` argument.
Consider this simple example::

    from stdnet import odm

    class Element(odm.StdModel):
        name = odm.SymbolField()

    class CompositeElement(odm.StdModel):
        weight = odm.FloatField()

    class Composite(odm.StdModel):
        name = odm.SymbolField()
        elements = odm.ManyToManyField(Element, through=CompositeElement,
                                       related_name='composites')




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


.. module:: pulsar.apps.data.odm.manager

Manager
~~~~~~~~~~~~~~~

.. autoclass:: Manager
   :members:
   :member-order: bysource


.. module:: pulsar.apps.data.odm.mapper

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


.. module:: pulsar.apps.data.odm.searchengine

Search Engine
~~~~~~~~~~~~~~~~~~

.. autoclass:: SearchEngine
   :members:
   :member-order: bysource
