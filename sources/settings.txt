.. _settings:

.. module:: pulsar.utils.config

=======================
Settings
=======================

This is the full list of :ref:`pulsar settings <api-config>` available for
configuring your pulsar server or applications. Pulsar can
run several :class:`pulsar.apps.Application` in one server, via the
:class:`pulsar.apps.MultiApp` class. Each application has its own set
of configuration parameters with the only exception of the
:ref:`global server settings <setting-section-global-server-settings>`.


.. pulsar_settings::

Utilities
================

.. autofunction:: pass_through