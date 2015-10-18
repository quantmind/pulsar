.. _tutorials-django:

=======================
Django with Pulsar
=======================

This is a web chat application which illustrates how to run a django
site with pulsar and how to include pulsar asynchronous request middleware
into django.
The code for this example is located in the :mod:`examples.djchat`
module.

To run::

    python manage.py pulse

If running for the first time, issue the::

    python manage.py syncdb

command and create the super user.
This example uses the :ref:`django pulse <apps-pulse>` application.

Message and data backend
============================
By default, messages from connected (websocket) clients are synchronised via
the :ref:`pulsar data store <pulsar-data-store>` which starts when the django
site starts. It is possible to specify a different data store via the
:ref:`data-store option <setting-data_store>`.For example, it is possible
to use redis_ as an alternative data store
by issuing the following start up command::

    python manage.py pulse --data-store redis://127.0.0.1:6379/3

Views and Middleware
==========================
Check the :mod:`~examples.djchat.djchat.settings` module to see how this
classes are used.

.. automodule:: examples.djchat.djchat.views
   :members:
   :member-order: bysource


.. _redis: http://redis.io/
