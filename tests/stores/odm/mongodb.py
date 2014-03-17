import unittest

from pulsar import new_event_loop, data_stores

from . import Odm


@unittest.skipUnless('mongodb' in data_stores,
                     'Requires pymong and a running mongodb')
class TestMongodbODM(Odm, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        addr = 'redis://%s' % cls.cfg.mongodb_server
        cls.store = cls.create_store(addr)
        cls.sync_store = cls.create_store(addr, loop=new_event_loop())
