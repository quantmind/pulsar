'''
Collection of data structures and function used throughout the library.

.. module:: pulsar.utils.structures.misc

MultiValueDict
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: MultiValueDict
   :members:
   :member-order: bysource


.. _attribute-dictionary:

AttributeDictionary
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AttributeDictionary
   :members:
   :member-order: bysource


FrozenDict
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: FrozenDict
   :members:
   :member-order: bysource


.. module:: pulsar.utils.structures.skiplist

Skiplist
~~~~~~~~~~~~~~~
.. autoclass:: Skiplist
   :members:
   :member-order: bysource


.. module:: pulsar.utils.structures.zset

Zset
~~~~~~~~~~~~~~~
.. autoclass:: Zset
   :members:
   :member-order: bysource
'''
from .skiplist import Skiplist
from .zset import Zset
from .misc import (
    AttributeDictionary, FrozenDict, Dict, Deque, recursive_update,
    mapping_iterator, inverse_mapping, aslist, as_tuple
)


__all__ = [
    'Skiplist',
    'Zset',
    'AttributeDictionary',
    'FrozenDict',
    'Dict',
    'Deque',
    'recursive_update',
    'mapping_iterator',
    'inverse_mapping',
    'aslist',
    'as_tuple'
]
