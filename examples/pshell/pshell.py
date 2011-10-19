'''\
Pulsar Python Shell example::

    python pshell.py
'''
import os
import pulsar

from pulsar.apps.shell import PulsarShell 

if __name__ == '__main__':
    PulsarShell().start()
