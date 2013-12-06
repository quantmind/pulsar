'''
Pulsar ships with an ultrafast object data mapper (ODM) for managing
and retrieving data asynchronously.

The ODM, built on top of pulsar :ref:`data store client api <data-stores>`,
presents a method of associating user-defined Python classes with data-stores
**collections** (or **tables** in SQL world), and instances of those
classes with **items** (**rows** in SQL) in their corresponsing
collections.
'''
from .fields import (Field, CharField, FloatField, IntegerField, PickleField,
                     AutoIdField)
from .model import Model, PyModel
from .mapper import Mapper, Manager
