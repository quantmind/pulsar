#!/usr/bin/env python
'''This a web chat application which illustrates how to run a django site with
pulsar and how to include pulsar asynchronous request middlewares into django.
Requires django 1.4 or above. To run::

    python manage.py pulse

If running for the first time, issue the::

    python manage.py syncdb

command first and create the super user. The example defines two django applications:

* :ref:`djpulsar <djpulsar>` which implements the django ``pulse`` command.
* ``djangoapp`` which runs the actual example. 


.. _djpulsar:

djpulsar app
========================

.. automodule:: examples.djangoapp.djpulsar
'''
import os, sys
try:
    import pulsar
except ImportError: #pragma nocover
    import sys
    sys.path.append('../../')
    
if __name__ == "__main__":
    os.environ["DJANGO_SETTINGS_MODULE"] = "djangoapp.settings"
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)