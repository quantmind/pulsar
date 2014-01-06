#!/usr/bin/env python
'''This is a web chat application which illustrates how to run a django
site with pulsar and how to include pulsar asynchronous request middlewares
into django.
Requires django 1.4 or above. To run::

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
:ref:`data-store option <setting-data_store>`.

For example, it is possible to use redis_ as an alternative data store
simply by issuing the following start up command::

    python manage.py pulse --data-store redis://127.0.0.1:6379/3


Views and Middleware
==========================

Check the :mod:`examples.djangoapp.chat.settings` module to see how this
classes are used.

.. automodule:: examples.djangoapp.chat.views
   :members:
   :member-order: bysource


.. _redis: http://redis.io/
'''
import os
try:
    import pulsar
except ImportError:     # pragma nocover
    import sys
    sys.path.append('../../')

os.environ["DJANGO_SETTINGS_MODULE"] = "chat.settings"
from django.core.management import execute_from_command_line


if __name__ == "__main__":
    execute_from_command_line()
