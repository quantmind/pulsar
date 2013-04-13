'''
Constats used throughout pulsar.

.. _actor-states:

Actor States
~~~~~~~~~~~~~~

Actor state constants are access via::

    from pulsar import ACTOR_STATES
    
They are:  

* ``ACTOR_STATES.INITIAL = 0`` when an actor is just created, before the
  :class:`pulsar.Actor.start` method is called.
* ``ACTOR_STATES.STARTING = 2`` when :class:`pulsar.Actor.start` method is called.
* ``ACTOR_STATES.RUN = 3`` when :class:`pulsar.Actor.event_loop` is up and running.
* ``ACTOR_STATES.STOPPING = 4`` when :class:`pulsar.Actor.stop` has been called
  for the first time and the actor is running.
  
Constants
~~~~~~~~~~~~~~~~~~~~~~~~

* ``ACTOR_ACTION_TIMEOUT`` used by :class:`pulsar.Monitor` to kill actors
  which don't respond to the stop command.

'''
from pulsar.utils.structures import AttributeDictionary, FrozenDict

# LOW LEVEL CONSTANTS - NO NEED TO CHANGE THOSE ###########################
ACTOR_STATES = AttributeDictionary(INITIAL=0X0,
                                   INACTIVE=0X1,
                                   STARTING=0x2,
                                   RUN=0x3,
                                   STOPPING=0x4,
                                   CLOSE=0x5,
                                   TERMINATE=0x6)
ACTOR_STATES.DESCRIPTION = {ACTOR_STATES.INACTIVE: 'inactive',
                            ACTOR_STATES.INITIAL: 'initial',
                            ACTOR_STATES.STARTING: 'starting',
                            ACTOR_STATES.RUN: 'running',
                            ACTOR_STATES.STOPPING: 'stopping',
                            ACTOR_STATES.CLOSE: 'closed',
                            ACTOR_STATES.TERMINATE:'terminated'}
SPECIAL_ACTORS = ('monitor', 'arbiter')
#
# TIMEOUT FOR WHEN AN ACTOR IS NOT RESPONSING TO A STOP COMMAND
ACTOR_ACTION_TIMEOUT = 5
MIN_NOTIFY = 3     # DON'T NOTIFY BELOW THIS INTERVAL
MAX_NOTIFY = 30    # NOTIFY AT LEAST AFTER THESE SECONDS
ACTOR_TIMEOUT_TOLE = 0.3  # NOTIFY AFTER THIS TIMES THE TIMEOUT
ACTOR_TERMINATE_TIMEOUT = 2 # TIMEOUT WHEN JOINING A TERMINATING ACTOR
#
# SPECIAL objects for Deferred
CONTINUE = object()
NOT_DONE = object()
CLEAR_ERRORS = object()
#
# Globals
EMPTY_TUPLE = ()
EMPTY_DICT = FrozenDict()
