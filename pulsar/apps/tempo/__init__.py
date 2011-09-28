'''\
Pulsar tempo is an parallel testing/benchmarking/profiling application
sending lots of requests, and analysing results.
It is used to benchmark pulsar itself.
'''
import pulsar

from .config import *


def basescript():
    pass


class Application(pulsar.Application):
    '''A benchmarking application for testing http servers'''
    