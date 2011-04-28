'''\
Pulsar Python Shell
'''
import os
try:
    from penv import pulsar
except ImportError:
    import pulsar

from pulsar.apps.shell import PulsarShell 

if __name__ == '__main__':
    PulsarShell(concurrency = 'thread').start()
