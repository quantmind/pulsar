'''\
Pulsar tempo is an application for benchmarking application servers by
sending lots of requests, and analysing results.
It is used to benchmark pulsar itself.
'''
import pulsar

from .config import *


def basescript():
    pass


class Application(pulsar.Application):
    '''A benchmarking application for testing http servers'''
    