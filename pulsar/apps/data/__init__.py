'''
A pulsar application for managing asynchronous clients to remote data-stores::

    from pulsar.apps.data import create_store

    redis_store = create_store('redis://127.0.0.1:6379/9')


Includes a redis-like server which can be used as stand-alone application.
'''
from .server import *
from .stores import *
