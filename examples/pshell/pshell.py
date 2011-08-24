'''\
Pulsar Python Shell
'''
import os
import pulsar

from pulsar.apps.shell import PulsarShell 

if __name__ == '__main__':
    PulsarShell(concurrency = 'thread').start()
