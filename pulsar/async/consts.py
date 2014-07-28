'''
Constants used throughout pulsar.
'''
from pulsar.utils.structures import AttributeDictionary

# LOW LEVEL CONSTANTS - NO NEED TO CHANGE THOSE ###########################
ACTOR_STATES = AttributeDictionary(INITIAL=0X0,
                                   INACTIVE=0X1,
                                   STARTING=0x2,
                                   RUN=0x3,
                                   STOPPING=0x4,
                                   CLOSE=0x5,
                                   TERMINATE=0x6)
'''
.. _actor-states:

Actor state constants are access via::

    from pulsar import ACTOR_STATES

They are:

* ``ACTOR_STATES.INITIAL = 0`` when an actor is just created, before the
  :class:`pulsar.Actor.start` method is called.
* ``ACTOR_STATES.STARTING = 2`` when :class:`pulsar.Actor.start` method
  is called.
* ``ACTOR_STATES.RUN = 3`` when :class:`pulsar.Actor._loop` is up
  and running.
* ``ACTOR_STATES.STOPPING = 4`` when :class:`pulsar.Actor.stop` has been
  called for the first time and the actor is running.
'''
ACTOR_STATES.DESCRIPTION = {ACTOR_STATES.INACTIVE: 'inactive',
                            ACTOR_STATES.INITIAL: 'initial',
                            ACTOR_STATES.STARTING: 'starting',
                            ACTOR_STATES.RUN: 'running',
                            ACTOR_STATES.STOPPING: 'stopping',
                            ACTOR_STATES.CLOSE: 'closed',
                            ACTOR_STATES.TERMINATE: 'terminated'}
#
ACTOR_ACTION_TIMEOUT = 5
'''Important constant used by :class:`pulsar.Monitor` to kill actors which
don't respond to the ``stop`` command.'''

MAX_ASYNC_WHILE = 1     # Max interval for async_while
MIN_NOTIFY = 3     # DON'T NOTIFY BELOW THIS INTERVAL
MAX_NOTIFY = 30    # NOTIFY AT LEAST AFTER THESE SECONDS
ACTOR_TIMEOUT_TOLE = 0.3  # NOTIFY AFTER THIS TIMES THE TIMEOUT
ACTOR_JOIN_THREAD_POOL_TIMEOUT = 5  # TIMEOUT WHEN JOINING THE THREAD POOL
MONITOR_TASK_PERIOD = 1
'''Interval for :class:`pulsar.Monitor` and :class:`pulsar.Arbiter`
periodic task.'''
