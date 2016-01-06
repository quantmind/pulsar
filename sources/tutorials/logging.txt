.. _tutorials-logging:

==================
Logging
==================

Pulsar provides several :ref:`settings <settings>` for managing the python logging module and display information when running. These configuration parameters can be specified both on the command line
or in the :ref:`config <setting-config>` file of your application.

The :ref:`log-level <setting-loglevel>` sets levels of loggers, for example::

	python script.py --log-level debug

Set the log level for the ``pulsar.*`` loggers to ``DEBUG``.

You can pass several namespaces to the command, for example::

	python script.py --log-level warning pulsar.wsgi.debug
