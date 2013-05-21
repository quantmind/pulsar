#!/usr/bin/env python
'''This a web chat application which illustrates how to run a django site with
pulsar and how to include pulsar asynchronous request middlewares into django.
Requires django 1.4 or above. To run::

    python manage.py pulse

If running for the first time, issue the::

    python manage.py syncdb

command and create the super user.
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