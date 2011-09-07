'''\
Pulsar temp is an application for benchmarking applications by sending lots
of requests, and analysing results. It is used to benchmark pulsar itself.
'''
import pulsar

from .config import *


def basescript():
    pass


class Application(pulsar.Application):
    pass