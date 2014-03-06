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
from collections import *

from .skiplist import Skiplist
from .zset import Zset
from .misc import (MultiValueDict, AttributeDictionary, FrozenDict,
                   Dict, Deque, merge_prefix, recursive_update,
                   mapping_iterator, inverse_mapping, isgenerator,
                   aslist)
