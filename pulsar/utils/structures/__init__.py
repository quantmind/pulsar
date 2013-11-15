from collections import *

from ..pep import ispy26

if ispy26:    # pragma    nocover
    from ..fallbacks._collections import *

from .hash import Hash
from .skiplist import Skiplist
from .zset import Zset
from .structures import (MultiValueDict, AttributeDictionary, FrozenDict,
                         Deque, merge_prefix, recursive_update,
                         mapping_iterator, inverse_mapping, isgenerator,
                         aslist)
