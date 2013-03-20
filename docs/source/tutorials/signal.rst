.. _tutorials-signal:

=======================
Signal Handling
=======================


Handling of SIGTERM
=========================
The SIGTERM signals tells pulsar to shutdown gracefully. When this signal is
received, the arbiter schedules a shutdown very similar to the one performed
when the :ref:`stop command <actor_stop_command>` is called.
The scheduled shutdown starts ASAP, 