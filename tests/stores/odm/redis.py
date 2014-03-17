from pulsar import new_event_loop

from .pulsards import unittest, StoreMixin, Odm


class TestRedisODM(StoreMixin, Odm, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        addr = 'redis://%s' % cls.cfg.redis_server
        cls.store = cls.create_store(addr)
        cls.sync_store = cls.create_store(addr, loop=new_event_loop())
