#!/usr/bin/env python
import os
import sys

from django.core.management import execute_from_command_line


def server(argv=None):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djchat.settings")
    execute_from_command_line(argv or sys.argv)


if __name__ == "__main__":  # pragma    nocover
    server()
