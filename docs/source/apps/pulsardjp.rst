.. _apps-pulsardjp:

.. module:: pulsar.apps.pulsardjp

=============================
Djpcms-Pulsar Integration
=============================

A pulsar application for serving djpcms_ powered web sites and
displaying information about pulsar servers.
It includes an :class:`pulsar.apps.tasks.Task` implementation
with Redis backend which uses stdnet_.

To use it:

* Add ``pulsar.apps.pulsardjp`` to the list of ``INSTALLED_APPS``.
* type::

    python manage.py run_pulsar


.. _djpcms: https://github.com/lsbardel/djpcms
.. _stdnet: http://lsbardel.github.com/python-stdnet/



Utilities
===================

SiteLoader
~~~~~~~~~~~~~~~~~

.. autoclass:: SiteLoader
   :members:
   :member-order: bysource