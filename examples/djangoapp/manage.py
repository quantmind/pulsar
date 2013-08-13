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

Views and Middleware
==========================

Check the :mod:`examples.djangoapp.chat.settings` module to see how this classes
are used.

.. automodule:: examples.djangoapp.chat.views
   :members:
   :member-order: bysource
   
'''
import os, sys
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
os.environ["DJANGO_SETTINGS_MODULE"] = "chat.settings"
from django.core.management import execute_from_command_line
    
if __name__ == "__main__":
    execute_from_command_line()