#!/usr/bin/env python
'''Requires django 1.4 or above.'''
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