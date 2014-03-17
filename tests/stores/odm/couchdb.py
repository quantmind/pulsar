import unittest

from pulsar import new_event_loop, data_stores
from pulsar.apps.test import check_server

from . import Odm
from ..couchdb import OK, TestStoreWithDb


@unittest.skipUnless(OK, 'Requires a running CouchDB server')
class TestCouchdbODM(Odm, TestStoreWithDb, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        addr = '%s/%s' % (cls.cfg.couchdb_server, cls.name('odm'))
        cls.store = cls.create_store(addr)
        cls.sync_store = cls.create_store(addr, loop=new_event_loop())
        return cls.store.create_database()

    @classmethod
    def tearDownClass(cls):
        return cls.store.delete_database()
