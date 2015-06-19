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
from collections import *       # noqa

from .skiplist import Skiplist  # noqa
from .zset import Zset          # noqa
from .misc import (MultiValueDict, AttributeDictionary, FrozenDict,  # noqa
                   Dict, Deque, merge_prefix, recursive_update,  # noqa
                   mapping_iterator, inverse_mapping, aslist)    # noqa
