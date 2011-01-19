"""
kombu.transport.base
====================

Base transport interface.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""

from kombu import serialization
from kombu.compression import decompress
from kombu.exceptions import MessageStateError

ACKNOWLEDGED_STATES = frozenset(["ACK", "REJECTED", "REQUEUED"])


