#!/usr/bin/env python
'''This application requires django 1.4 or above. To run this example type::

    python manage.py pulse

If running for the first time, issue the::

    python manage.py syncdb

command first and create the super user.

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